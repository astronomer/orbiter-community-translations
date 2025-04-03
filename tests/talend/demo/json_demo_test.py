from orbiter_translations.talend.json_demo import translation_ruleset


def test_talend_demo(project_root):
    actual = translation_ruleset.translate_fn(
        translation_ruleset, (project_root / "tests/talend/demo/workflow/")
    )
    assert list(actual.dags.keys()) == ["pipeline_2"]
    assert sorted(list(list(actual.dags.values())[0].tasks.keys())) == sorted(
        [
            "airlines",
            "foo",
            "python_3_1"
        ]
    )
    assert sorted(
        list(list(actual.dags.values())[0].tasks["foo"].downstream)
    ) == sorted(["python_3_1"])
