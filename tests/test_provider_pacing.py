"""Offline experimental pacing, error metadata and JSON-mode tests."""
import asyncio
import io
import json
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from labtune.providers.chat import ChatCompletionsClient
from labtune.providers.http import ProviderError
from labtune.providers.configuration import generation_settings

class ProviderPacingTests(unittest.TestCase):
 def client(self,**kw):return ChatCompletionsClient(None,model='offline',base_url='https://offline.invalid',max_parallel=1,**kw)
 def reply(self):return {'choices':[{'message':{'content':'OK'},'finish_reason':'stop'}]}
 async def ask(self,c):return await c.generate([{'role':'user','content':'offline'}],budget_ref='offline')
 def test_default_gap_is_zero(self):self.assertEqual(self.client(transport=lambda *a:self.reply()).request_gap_seconds,0)
 def test_invalid_gap_rejected(self):
  cfg={'base_url':'https://offline.invalid','model':'offline','api_key_env':None,'env_file':'.env','timeout_seconds':1,'max_output_tokens':1,'max_parallel_requests':1}
  for value in [-1,True,float('inf'),'10']:
   with self.subTest(value=value),self.assertRaises(ProviderError):generation_settings(dict(cfg,request_gap_seconds=value))
 def test_gap_is_after_completion_not_start(self):
  starts=[];ends=[]
  def transport(*args):starts.append(time.monotonic());time.sleep(.01);ends.append(time.monotonic());return self.reply()
  async def run():
   c=self.client(transport=transport,request_gap_seconds=.04)
   await asyncio.gather(self.ask(c),self.ask(c))
  asyncio.run(run());self.assertEqual(len(starts),2);self.assertGreaterEqual(starts[1]-ends[0],.03)
 def test_cancellation_while_waiting_does_not_reserve_or_dispatch(self):
  counts={'reserve':0,'dispatch':0}
  def reserve():counts['reserve']+=1
  def transport(*args):counts['dispatch']+=1;return self.reply()
  async def run():
   c=self.client(transport=transport,reserve=reserve,request_gap_seconds=.2)
   await self.ask(c);task=asyncio.create_task(self.ask(c));await asyncio.sleep(.01);task.cancel()
   with self.assertRaises(asyncio.CancelledError):await task
  asyncio.run(run());self.assertEqual(counts,{'reserve':1,'dispatch':1})
 def test_429_keeps_bounded_code_retry_after_and_not_message(self):
  journal=[];body=json.dumps({'error':{'code':'1302','message':'DO-NOT-LOG-PRIVATE-MESSAGE'}}).encode()
  class Opener:
   def open(self,request,timeout):raise HTTPError(request.full_url,429,'fixture',{'Retry-After':'30'},io.BytesIO(body))
  with patch('labtune.providers.http.urllib.request.build_opener',return_value=Opener()):
   with self.assertRaises(ProviderError) as caught:asyncio.run(self.ask(self.client(on_event=journal.append)))
  self.assertEqual(caught.exception.kind,'rate_limit');self.assertEqual(caught.exception.provider_error_code,'1302');self.assertEqual(caught.exception.retry_after_seconds,30)
  self.assertEqual(journal[-1]['provider_error_code'],'1302');self.assertNotIn('DO-NOT-LOG-PRIVATE-MESSAGE',json.dumps(journal))
 def test_invalid_error_body_does_not_hide_http_status(self):
  class Opener:
   def open(self,request,timeout):raise HTTPError(request.full_url,429,'fixture',{'Retry-After':'invalid'},io.BytesIO(b'not json'))
  with patch('labtune.providers.http.urllib.request.build_opener',return_value=Opener()):
   with self.assertRaises(ProviderError) as caught:asyncio.run(self.ask(self.client()))
  self.assertEqual(caught.exception.http_status,429);self.assertIsNone(caught.exception.retry_after_seconds)
 def test_explicit_json_mode_sent_without_identity_override(self):
  bodies=[]
  def transport(method,path,body):bodies.append(body);return self.reply()
  c=self.client(transport=transport,request_options={'response_format':{'type':'json_object'}})
  asyncio.run(self.ask(c));self.assertEqual(bodies[0]['response_format'],{'type':'json_object'});self.assertEqual(bodies[0]['model'],'offline')
