from orbiter_translations.oozie.xml_demo import translation_ruleset


def test_oozie_demo(project_root):
    actual = translation_ruleset.translate_fn(
        translation_ruleset, (project_root / "tests/oozie/demo/workflow/")
    )
    assert list(actual.dags.keys()) == ["demo_wf"]
    assert sorted(list(list(actual.dags.values())[0].tasks.keys())) == sorted(
        ["cleanup_node", "end", "fail", "hdfs_node", "start"]
    )
    assert sorted(
        list(list(actual.dags.values())[0].tasks["hdfs_node"].downstream)
    ) == sorted(["end", "fail"])
