"""
candata_pipeline.pipelines — End-to-end pipeline orchestrators.

Each pipeline module exports a run() async function that accepts
RunConfig kwargs and returns a LoadResult.

    from candata_pipeline.pipelines import economic_pulse, housing, procurement, trade

    result = await economic_pulse.run(start_date=date(2020, 1, 1))
"""
