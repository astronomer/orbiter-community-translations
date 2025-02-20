from orbiter_translations.argo.yaml_demo import translation_ruleset


def test_argo_demo(project_root):
    actual = translation_ruleset.translate_fn(
        translation_ruleset, (project_root / "tests/argo/demo/workflow/")
    )
    assert list(actual.dags.keys()) == ["dag_diamond"]
    assert sorted(list(list(actual.dags.values())[0].tasks.keys())) == sorted(["echo"])
