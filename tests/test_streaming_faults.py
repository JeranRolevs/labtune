"""Experimental offline fault/stop/accounting checks. Never contact a provider."""
import asyncio
from collections import Counter
from http.client import IncompleteRead
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from labtune.providers.chat import ChatCompletionsClient
from labtune.providers.http import ProviderError
from labtune.study import TextStudy, StudyError

WIRE = b'data: {"choices":[{"index":0,"delta":{"content":"OK"},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n'


class StreamingFaultTests(unittest.TestCase):
    def setUp(self):
        self.network = patch('urllib.request.OpenerDirector.open', side_effect=AssertionError('Network forbidden in offline tests'))
        self.network.start()
    def tearDown(self):
        self.network.stop()
    def client(self, **kwargs):
        return ChatCompletionsClient(None, model='offline-fixture', base_url='https://offline.invalid', stream=True, max_parallel=1, **kwargs)
    def ask(self, client):
        return client.generate([{'role':'user','content':'offline fixture'}], budget_ref='offline')
    def test_502_has_one_dispatch_and_no_retry(self):
        calls=[];journal=[]
        class Opener:
            def open(self, request, timeout):
                calls.append(1)
                raise HTTPError(request.full_url,502,'offline failure',{},None)
        with patch('labtune.providers.http.urllib.request.build_opener', return_value=Opener()):
            with self.assertRaises(ProviderError) as caught:asyncio.run(self.ask(self.client(on_event=journal.append)))
        self.assertEqual(caught.exception.http_status,502)
        self.assertEqual(len(calls),1)
        self.assertEqual([x['kind'] for x in journal],['provider_request','provider_error'])
    def test_stall_reset_and_incomplete_read_fail_without_success(self):
        for error in [TimeoutError('offline stall'),ConnectionResetError('offline reset'),IncompleteRead(b'partial',7)]:
            with self.subTest(error=type(error).__name__):
                journal=[];closed=[]
                class Response(io.BytesIO):
                    headers={'Content-Type':'text/event-stream'}
                    def readline(self, *args):raise error
                    def close(self):closed.append(True);super().close()
                class Opener:
                    def open(self,*args,**kwargs):return Response()
                with patch('labtune.providers.http.urllib.request.build_opener',return_value=Opener()):
                    with self.assertRaises(ProviderError) as caught:asyncio.run(self.ask(self.client(on_event=journal.append)))
                self.assertEqual(caught.exception.kind,'transport_uncertain')
                self.assertTrue(closed)
                self.assertFalse(any(x['kind']=='provider_response' for x in journal))
    def test_truncated_body_does_not_become_a_response(self):
        journal=[]
        class Response(io.BytesIO):headers={'Content-Type':'text/event-stream'}
        class Opener:
            def open(self,*args,**kwargs):return Response(WIRE.split(b'data: [DONE]')[0])
        with patch('labtune.providers.http.urllib.request.build_opener',return_value=Opener()):
            with self.assertRaises(ProviderError):asyncio.run(self.ask(self.client(on_event=journal.append)))
        self.assertFalse(any(x['kind']=='provider_response' for x in journal))
    def meter(self, receipt):
        with tempfile.TemporaryDirectory() as td:
            s=object.__new__(TextStudy);s.root=Path(td);s.events=0;s._secrets=set();s.progress=None;s.event_sink=None;s.decision_usage={}
            s.usage={'chat_input_tokens':0,'chat_output_tokens':0,'decision_input_tokens':0,'decision_output_tokens':0,'missing_usage_receipts':0}
            s.event({'kind':'provider_response','provider':'generation','response':{'usage':receipt}})
            return s.usage
    def test_missing_entire_usage_is_not_free_receipt(self):
        self.assertEqual(self.meter({})['missing_usage_receipts'],1)
    def test_missing_output_usage_is_flagged(self):
        m=self.meter({'prompt_tokens':3})
        self.assertEqual(m['chat_input_tokens'],3)
        self.assertEqual(m['missing_usage_receipts'],1)
    def test_missing_input_usage_preserves_known_output(self):
        m=self.meter({'completion_tokens':7})
        self.assertEqual(m['chat_output_tokens'],7)
        self.assertEqual(m['missing_usage_receipts'],1)
    def test_invalid_output_usage_is_flagged(self):
        for value in [-1,True,'unknown']:
            with self.subTest(value=value):self.assertEqual(self.meter({'prompt_tokens':3,'completion_tokens':value})['missing_usage_receipts'],1)
    def test_cancellation_does_not_promote_background_thread_result(self):
        entered=threading.Event();release=threading.Event();finished=threading.Event();journal=[];calls=[]
        def transport(*args):
            calls.append(1);entered.set()
            release.wait(2)
            finished.set()
            return {'choices':[{'message':{'content':'late result'},'finish_reason':'stop'}]}
        async def run():
            task=asyncio.create_task(self.ask(self.client(transport=transport,on_event=journal.append)))
            for _ in range(100):
                if entered.is_set():break
                await asyncio.sleep(.005)
            self.assertTrue(entered.is_set())
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
            # Cancellation of an await does not terminate a blocking HTTP thread.
            # Record this limitation; do not claim remote inference/billing stopped.
            self.assertFalse(finished.is_set())
            release.set()
            for _ in range(100):
                if finished.is_set():break
                await asyncio.sleep(.005)
        try:asyncio.run(run())
        finally:release.set()
        self.assertEqual(len(calls),1)
        self.assertFalse(any(x['kind']=='provider_response' for x in journal))
    def test_serial_semaphore_never_overlaps_completed_requests(self):
        lock=threading.Lock();counts=Counter()
        def transport(*args):
            with lock:counts['active']+=1;counts['peak']=max(counts['peak'],counts['active']);counts['calls']+=1
            time.sleep(.02)
            with lock:counts['active']-=1
            return {'choices':[{'message':{'content':'OK'},'finish_reason':'stop'}]}
        async def run():
            client=self.client(transport=transport)
            await asyncio.gather(self.ask(client),self.ask(client))
        asyncio.run(run())
        self.assertEqual(counts['peak'],1);self.assertEqual(counts['calls'],2)
    def test_partial_wave_preserves_previous_and_current_successes_once(self):
        study=object.__new__(TextStudy);study.outputs=[];study.event=lambda x:None
        study.plan={'resources':{'max_active_agents':2},'work_graph':[
            {'id':'one','kind':'episode','depends_on':[],'research_activity':'allowed','actors':['earlier']},
            {'id':'two','kind':'episode','depends_on':['one'],'research_activity':'allowed','actors':['current','bad']}]}
        async def run(node,actor):
            if actor=='bad':raise StudyError('offline failure')
            return {'actor':actor,'output':{'summary':'retained'}}
        study.run_node_episode=run
        with self.assertRaises(StudyError):asyncio.run(study.run_graph())
        self.assertEqual([x['actor'] for x in study.outputs],['earlier','current'])
    @unittest.skipUnless(os.name=='posix','POSIX process-group watchdog fixture')
    def test_hard_watchdog_can_stop_stalled_local_worker(self):
        start=time.monotonic()
        child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)'],start_new_session=True,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            with self.assertRaises(subprocess.TimeoutExpired):child.wait(timeout=.1)
            os.killpg(child.pid,signal.SIGKILL)
            child.communicate(timeout=2)
            self.assertIsNotNone(child.returncode)
            self.assertLess(time.monotonic()-start,3)
        finally:
            if child.poll() is None:os.killpg(child.pid,signal.SIGKILL);child.communicate(timeout=2)
