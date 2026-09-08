# Charmed Valkey documentation

```{warning}
The charm is under active development and is not yet production-ready.
APIs, configuration options, and integration endpoints may change.
{doc}`Contact us<reference/contact>` for feature requests.
```

Charmed Valkey is an open-source **Juju charm** that will automate the deployment, scaling,
configuration and operations of [Valkey](https://valkey.io/) clusters across clouds,
virtual machines and bare metal, using the [Juju](https://juju.is/) orchestration framework.

[Valkey](https://valkey.io/) is a community-driven, open-source, high-performance
key-value data store compatible with Redis® clients and ecosystem tooling.

The charm aims to simplify Valkey operations from
[Day 0 to Day 2](https://codilime.com/blog/day-0-day-1-day-2-the-software-lifecycle-in-the-cloud-age/),
offering secure defaults
integration interfaces, and lifecycle automation.

## In this documentation

|                                                                                                        |                                                                                               |
|--------------------------------------------------------------------------------------------------------| --------------------------------------------------------------------------------------------- |
| [Tutorial](/tutorial.md)</br>  Get started - a hands-on introduction to Valkey for new users </br>     | [How-to guides](/how-to/index.md) </br> Step-by-step guides covering key operations and common tasks |
<!--| [Reference](/reference/index) </br> Technical information - specifications, APIs, architecture  | [Explanation](/explanation/index.md) </br> Concepts - discussion and clarification of key topics |-->

## How this documentation is organised

This documentation uses the [Diátaxis](https://diataxis.fr/) documentation structure.

- The Tutorial takes you step-by-step through the initial Charmed Valkey setup and usage.
- How-to guides assume you have basic familiarity with Valkey and Juju.
- Reference includes factual data required for using Charmed Valkey.
- Explanation provides deeper understanding of important concepts and ideas.

## Project and community

Charmed Valkey is a member of the Ubuntu family.
It’s an open source project that warmly welcomes community contributions,
suggestions, fixes and constructive feedback.

- [Juju](https://juju.is/)
- [Charmhub](https://charmhub.io/)
- [Valkey](https://valkey.io/)
- [Valkey artifacts](https://github.com/canonical/valkey-artifacts)
- [Canonical Data solutions](https://canonical.com/data)
- [Report issues](https://github.com/canonical/valkey-operator/issues)
- {doc}`Contact us <reference/contact>`

## License and trademarks

Valkey and the Valkey logo are trademarks of LF Projects, LLC.

The Charmed Valkey operator is free software, distributed under the Apache Software License,
version 2.0.
See [LICENSE](https://github.com/canonical/valkey-operator/blob/main/LICENSE)
for more information.

```{toctree}
:titlesonly:
:maxdepth: 2
:hidden:

Home <self>
Tutorial <tutorial>
/how-to/index
Contact <reference/contact>
```
