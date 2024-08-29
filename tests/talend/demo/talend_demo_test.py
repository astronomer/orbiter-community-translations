from orbiter_translations.talend.json_demo import translation_ruleset


def test_talend_demo(project_root):
    actual = translation_ruleset.translate_fn(
        translation_ruleset, (project_root / "tests/talend/demo/workflow/")
    )
    assert list(actual.dags.keys()) == ["..."]
    assert sorted(list(list(actual.dags.values())[0].tasks.keys())) == sorted(
        [
            "...",
        ]
    )
    assert sorted(
        list(list(actual.dags.values())[0].tasks["..."].downstream)
    ) == sorted(["..."])
