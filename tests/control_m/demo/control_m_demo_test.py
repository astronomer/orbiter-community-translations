from orbiter_translations.control_m.xml_demo import translation_ruleset


def test_control_m_demo(project_root):
    actual = translation_ruleset.translate_fn(
        translation_ruleset, (project_root / "tests/control_m/demo/workflow/")
    )
    assert list(actual.dags.keys()) == ["demo"]
    assert sorted(list(list(actual.dags.values())[0].tasks.keys())) == sorted(
        [
            "task_a",
            "task_b",
        ]
    )
    assert sorted(
        list(list(actual.dags.values())[0].tasks["task_a"].downstream)
    ) == sorted(["task_b"])
