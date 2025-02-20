from orbiter.objects.operators.bash import OrbiterBashOperator

from orbiter_translations.cron.crontab_base import translation_ruleset


def test_cron_demo(project_root):
    actual = translation_ruleset.translate_fn(
        translation_ruleset, (project_root / "tests/cron/demo/workflow/")
    )
    assert list(actual.dags.keys()) == ["cron_0", "cron_1", "cron_2", "cron_3"]
    assert sorted(list(list(actual.dags.values())[0].tasks.keys())) == sorted(["cron_task"])
    assert isinstance(actual.dags["cron_0"].tasks["cron_task"], OrbiterBashOperator)
