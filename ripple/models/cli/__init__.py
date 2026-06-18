"""Out-of-suite command-line entry points for the RIPPLe model layer.

Run as modules (NO console-script entry points, so the base install keeps
``[project].dependencies == []``)::

    python -m ripple.models.cli.train --manifest path/to/manifest.csv
    python -m ripple.models.cli.evaluate --manifest path/to/manifest.csv --checkpoint ckpt.pt

Only ``build_parser()`` is exercised by the offline test suite; ``run()``
pulls torch and the trainer/evaluator and is run manually.
"""
