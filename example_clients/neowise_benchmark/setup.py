"""Build the native C row encoder for the NEOWISE benchmark.

The encoder is exposed as a submodule of the `neowise` package:
`neowise.neowise_native`. Run from the repo root:

    python3 setup.py build_ext --inplace

This drops `neowise/neowise_native.<ABI>.so` next to the rest of the package
and `native_encoder.py` can import it as `from neowise import neowise_native`.

In the published Dockerfile, the same command is run during image build.
"""

from setuptools import Extension, setup

setup(
    name="neowise-benchmark-native",
    version="0.1.0",
    description="Native C row encoder for the Zerobus NEOWISE benchmark",
    ext_modules=[
        Extension(
            "neowise.neowise_native",
            sources=["neowise/neowise_native.c"],
            extra_compile_args=["-O3", "-march=x86-64", "-fno-strict-aliasing"],
        )
    ],
)
