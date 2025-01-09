from orbiter.objects.operators.ssh import OrbiterSSHOperator

from orbiter_translations.autosys.jil_demo import translation_ruleset


def test_autosys_demo(project_root):
    actual = translation_ruleset.translate_fn(
        translation_ruleset, (project_root / "tests/autosys/demo/workflow/")
    )
    assert list(actual.dags.keys()) == ["foo_job"]
    assert sorted(list(actual.dags["foo_job"].tasks.keys())) == sorted(["foo_job",])
    assert isinstance(actual.dags["foo_job"].tasks["foo_job"], OrbiterSSHOperator)
