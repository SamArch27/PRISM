# PRISM

Instead of inlining an entire UDF into its calling query, we deconstruct a UDF into its constituent pieces and inline only the pieces that help query optimization. We achieve this through UDF outlining, which extracts non-declarative pieces of a UDF into separate functions, intentionally hiding irrelevant UDF code from the query optimizer through opaque function calls. In addition to UDF outlining, we implement four complementary UDF-centric optimizations in PRISM. By inlining only the pieces that help query optimization, PRISM generates simpler queries, resulting in signficantly faster query plans.

## Building

After cloning the repository, run `make init` in the root directory. 
