from spark.jobs.common_spark import get_spark

def test_session():
    s = get_spark("test")
    assert s is not None
