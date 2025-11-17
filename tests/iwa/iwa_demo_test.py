from orbiter_translations.iwa.txt_demo import translation_ruleset

def test_iwa_demo(project_root):
    actual = translation_ruleset.translate_fn(
        translation_ruleset, (project_root / "tests/iwa/demo/workflow/")
    )
    assert list(actual.dags.keys()) == ["sched_first1"]
    assert sorted(list(list(actual.dags.values())[0].tasks.keys())) == sorted(
        ["bar", "job_fta", "jobmdm"]
    )

    assert sorted(
        list(list(actual.dags.values())[0].tasks["jobmdm"].downstream)
    ) == ["bar", "job_fta"]
