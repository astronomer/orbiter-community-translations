from orbiter_translations.esp.txt_demo import translation_ruleset


def test_esp_demo(project_root):
    actual = translation_ruleset.translate_fn(
        translation_ruleset, (project_root / "tests/esp/demo/workflow/")
    )
    assert list(actual.dags.keys()) == ["payroll"]
    assert sorted(list(list(actual.dags.values())[0].tasks.keys())) == sorted(
        [
            "a",
            "b",
            "c",
            "d",
        ]
    )
    assert sorted(list(list(actual.dags.values())[0].tasks["a"].downstream)) == sorted(
        ["b", "c"]
    )
