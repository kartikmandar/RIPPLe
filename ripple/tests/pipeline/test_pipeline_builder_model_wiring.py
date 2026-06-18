"""pipeline_builder must construct ModelStage with the FULL config dict so the
nested model.type threads through (regression for the double-extraction bug)."""
from types import SimpleNamespace

from ripple.pipeline.pipeline_builder import PipelineBuilder
from ripple.pipeline.stages.model_stage import ModelStage


def test_builder_constructs_model_stage_with_full_dict():
    cfg = SimpleNamespace(
        name="t",
        model={"operation": "prediction", "type": "vit_binary",
               "params": {"task": "binary"}},
    )
    builder = PipelineBuilder(cfg)
    stages = builder._parse_stages_from_config()

    model_stages = [s for s in stages if isinstance(s, ModelStage)]
    assert len(model_stages) == 1
    stage = model_stages[0]
    # model_type threaded through from the nested 'model' block.
    assert stage.model_type == "vit_binary"
    assert stage.model_operation == "prediction"
    assert stage.model_params == {"task": "binary"}
    # The stage received the FULL config dict (has a top-level 'model' key).
    assert "model" in stage.config


def test_builder_model_stage_defaults_when_unspecified():
    cfg = SimpleNamespace(name="t", model={})
    builder = PipelineBuilder(cfg)
    stages = builder._parse_stages_from_config()
    stage = [s for s in stages if isinstance(s, ModelStage)][0]
    assert stage.model_type == "resnet_binary"
    assert stage.model_operation == "prediction"
