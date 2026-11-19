import importlib.util, tempfile, unittest
from pathlib import Path
from unittest.mock import patch, Mock
spec=importlib.util.spec_from_file_location('fork_console',Path(__file__).with_name('server.py'))
app=importlib.util.module_from_spec(spec);spec.loader.exec_module(app)

class ConsoleTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
  self.patch=patch.multiple(app,DATA=Path(self.tmp.name),DB=Path(self.tmp.name)/'state.sqlite',POOL=Mock());self.patch.start();self.addCleanup(self.patch.stop);app.init()
  app.put('sandboxes',dict(id='sb-test',name='sandbox',state='running',busy=False,image_id='python',image_name='python'))
 def test_paused_snapshot_is_rejected(self):
  app.update('sandboxes','sb-test',state='paused')
  with self.assertRaisesRegex(ValueError,'运行中'):app.submit('/api/snapshot',dict(sandbox_id='sb-test',name='name'))
  self.assertEqual(app.rows('snapshots'),[])
 def test_duplicate_is_rejected_before_capture(self):
  first=app.submit('/api/snapshot',dict(sandbox_id='sb-test',name='same / 中文'))
  app.update('sandboxes','sb-test',busy=False);app.update('snapshots',first['id'],state='ready')
  with self.assertRaisesRegex(ValueError,'同名'):app.submit('/api/snapshot',dict(sandbox_id='sb-test',name='same / 中文'))
  self.assertEqual(len(app.rows('snapshots')),1)
  self.assertNotIn('/',first['id'])
 def test_durable_state_not_downgraded_by_capture_reply(self):
  snap=app.submit('/api/snapshot',dict(sandbox_id='sb-test',name='state'))
  def reply(*a,**kw):app.update('snapshots',snap['id'],state='ready');return {'pause_ms':1}
  with patch.object(app,'admin',reply):app.capture_worker('sb-test',snap['id'])
  self.assertEqual(app.get('snapshots',snap['id'])['state'],'ready')
 def test_delete_keeps_live_descendant_backing(self):
  snap={'id':'sn-test','name':'snap','state':'ready'};app.put('snapshots',snap)
  app.update('sandboxes','sb-test',snapshot_id='sn-test')
  backing=app.DATA/'templates/sn-test';backing.mkdir(parents=True);(backing/'memory').write_bytes(b'data')
  app.submit('/api/delete-snapshot',dict(id='sn-test'));self.assertTrue(backing.exists())
  app.update('sandboxes','sb-test',state='deleted');app.purge_deleted();self.assertFalse(backing.exists())
 def test_no_image_deletion_endpoint(self):
  with self.assertRaises(ValueError):app.submit('/api/delete-image',dict(id='python'))
if __name__=='__main__':unittest.main()
