"""Offline error-stage/type diagnostics must never journal exception messages."""
import asyncio,io,json,ssl,unittest
from unittest.mock import patch
from urllib.error import URLError
from labtune.providers.http import ProviderError,safe_transport_details
from labtune.providers.chat import ChatCompletionsClient

class TransportDiagnosticsTests(unittest.TestCase):
 def test_url_reason_and_errno_without_secret_message(self):
  d=safe_transport_details(URLError(ConnectionResetError(104,'PRIVATE-KEY-IN-MESSAGE')),'read_stream')
  self.assertEqual(d['underlying_type'],'ConnectionResetError');self.assertEqual(d['errno'],104)
  self.assertNotIn('PRIVATE',json.dumps(d))
 def test_proxy_connect_status_only(self):
  d=safe_transport_details(URLError('Tunnel connection failed: 502 key=PRIVATE-SECRET'),'connect_or_headers')
  self.assertEqual(d['proxy_tunnel_http_status'],502);self.assertNotIn('PRIVATE',json.dumps(d))
 def test_ssl_failure_type_without_message(self):
  d=safe_transport_details(URLError(ssl.SSLCertVerificationError(1,'PRIVATE-CERT-TEXT')),'connect_or_headers')
  self.assertEqual(d['underlying_type'],'SSLCertVerificationError');self.assertNotIn('PRIVATE',json.dumps(d))
 def test_unknown_phase_is_not_logged_verbatim(self):
  d=safe_transport_details(TimeoutError('secret'),'PRIVATE-PHASE')
  self.assertEqual(d['phase'],'unknown');self.assertNotIn('PRIVATE',json.dumps(d))
 def test_connect_failure_metadata_survives_native_event(self):
  journal=[]
  class Opener:
   def open(self,*args,**kwargs):raise URLError(ConnectionResetError(104,'PRIVATE'))
  c=ChatCompletionsClient(None,model='fixture',base_url='https://offline.invalid',stream=True,on_event=journal.append)
  with patch('labtune.providers.http.urllib.request.build_opener',return_value=Opener()):
   with self.assertRaises(ProviderError):asyncio.run(c.generate([{'role':'user','content':'offline'}],budget_ref='test'))
  self.assertEqual(journal[-1]['transport_details']['phase'],'connect_or_headers');self.assertNotIn('PRIVATE',json.dumps(journal))
 def test_stream_read_stage_is_distinct(self):
  journal=[]
  class Response(io.BytesIO):
   headers={'Content-Type':'text/event-stream'}
   def readline(self,*args):raise ConnectionResetError(104,'PRIVATE')
  class Opener:
   def open(self,*args,**kwargs):return Response()
  c=ChatCompletionsClient(None,model='fixture',base_url='https://offline.invalid',stream=True,on_event=journal.append)
  with patch('labtune.providers.http.urllib.request.build_opener',return_value=Opener()):
   with self.assertRaises(ProviderError):asyncio.run(c.generate([{'role':'user','content':'offline'}],budget_ref='test'))
  self.assertEqual(journal[-1]['transport_details']['phase'],'read_stream');self.assertNotIn('PRIVATE',json.dumps(journal))
