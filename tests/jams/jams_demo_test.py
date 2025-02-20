from orbiter.objects.operators.ssh import OrbiterSSHOperator

from orbiter_translations.jams.xml_demo import translation_ruleset


def test_jams_demo(project_root):
    actual = translation_ruleset.translate_fn(
        translation_ruleset, (project_root / "tests/jams/demo/workflow/")
    )
    assert len(actual.dags.keys()) == 1
    actual_dag = list(actual.dags.values())[0]
    assert sorted(list(actual_dag.tasks.keys())) == sorted(
        ['bar', 'foo', 'email_notification']
    )
    assert isinstance(actual_dag.tasks["bar"], OrbiterSSHOperator)
    assert sorted(list(actual_dag.tasks["bar"].downstream)) == sorted(
        []
    )
