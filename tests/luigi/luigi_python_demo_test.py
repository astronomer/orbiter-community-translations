from orbiter_translations.luigi.python_demo import translation_ruleset


def test_luigi_python_demo(project_root):
    actual = translation_ruleset.translate_fn(
        translation_ruleset, (project_root / "tests/luigi/demo/workflow/")
    )

    assert list(actual.dags.keys()) == ["generate_words", "count_letters"]
    assert sorted(list(list(actual.dags.values())[0].tasks.keys())) == sorted(['run'])
