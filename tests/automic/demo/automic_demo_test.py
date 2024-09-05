from orbiter_translations.automic.xml_demo import translation_ruleset


def test_automic_demo(project_root):
    actual = translation_ruleset.translate_fn(
        translation_ruleset, (project_root / "tests/automic/demo/workflow/")
    )

    assert list(actual.dags.keys()) == ["jobp.dummy.workflow"]
    assert sorted(list(list(actual.dags.values())[0].tasks.keys())) == sorted(
        [
            "start",
            "generate_file",
            "process_and_upload_file",
            "insert_into_database",
            "end",
        ]
    )

    assert sorted(
        list(list(actual.dags.values())[0].tasks["insert_into_database"].downstream)
    ) == sorted(["end"])
