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
 def test_image_name_strips_only_organization_tag_and_digest(self):
  for source,want in [('python:3.12-slim','python'),('namanjain12/numpy_final:ff12','numpy_final'),('registry:5000/team/app@sha256:abc','app'),('swerebench/sweb.eval.x86_64.twisted_1776_twisted-12096','sweb.eval.x86_64.twisted_1776_twisted-12096')]:
   self.assertEqual(app.image_short_name(source),want)
 def test_snapshot_collision_is_regenerated_without_mutating_existing_names(self):
  app.put('snapshots',dict(id='old',name='snapshot-python-00007',state='deleted',parent='other'))
  with patch.object(app.secrets,'randbelow',side_effect=[7,7,12]):
   snap=app.submit('/api/snapshot',dict(sandbox_id='sb-test',name='ignored-client-name'))
  self.assertEqual(snap['name'],'snapshot-python-00012')
  self.assertEqual(app.get('snapshots','old')['name'],'snapshot-python-00007')
 def test_new_sandbox_has_short_unique_name(self):
  app.put('sandboxes',dict(id='old',name='sandbox-numpy_final-01234',state='deleted'))
  with patch.object(app,'image',return_value={'id':'numpy','name':'namanjain12/numpy_final:abc','state':'ready'}),patch.object(app.secrets,'randbelow',side_effect=[1234,981]):
   row=app.submit('/api/create',dict(kind='image',source='numpy'))
  self.assertEqual(row['name'],'sandbox-numpy_final-00981')
 def test_live_names_survive_init(self):
  before=app.rows('sandboxes');app.init();self.assertEqual(before,app.rows('sandboxes'))
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
