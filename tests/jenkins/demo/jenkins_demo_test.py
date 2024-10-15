from orbiter_translations.jenkins.json_demo import translation_ruleset


def test_jenkins_demo(project_root):
    actual = translation_ruleset.translate_fn(
        translation_ruleset, (project_root / "tests/jenkins/demo/workflow/")
    )

    assert list(actual.dags.keys()) == ["demo1"]
    assert sorted(list(list(actual.dags.values())[0].tasks.keys())) == sorted(
        [
            "build",
            "test",
            "deploy",
        ]
    )

    assert sorted(
        list(list(actual.dags.values())[0].tasks["build"].downstream)
    ) == sorted(["test"])
