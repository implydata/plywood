# Plywood

Plywood is a JavaScript library that simplifies building interactive
visualizations and applications for large data sets. Plywood acts as a
middle-layer between data visualizations and data stores.

Plywood is architected around the principles of nested
[Split-Apply-Combine](http://www.jstatsoft.org/article/view/v040i01/v40i01.pdf),
a powerful divide-and-conquer algorithm that can be used to construct all types
of data visualizations. Plywood comes with its own [expression
language](docs/expressions.md) where a single Plywood expression can
translate to multiple database queries, and where results are returned in a
nested data structure so they can be easily consumed by visualizaton libraries
such as [D3.js](http://d3js.org/). 

You can use Plywood in the browser and/or in node.js to easily create your own
visualizations and applications. For an example application built using
Plywood, please see [Pivot](https://github.com/implydata/pivot).

## Installation

To use Plywood from npm simply run: `npm install plywood`.

Plywood can also be used by the browser.

## Documentation

To learn more, see [http://plywood.imply.io](http://plywood.imply.io/)

## Also see

* [Pivot](https://github.com/implydata/pivot) - a data exploration GUI built using Plywood.
* [PlyQL](https://github.com/implydata/plyql) - a SQL-like interface to plywood
* [Plywood Proxy](https://github.com/implydata/plywood-proxy) - A handy proxy server for Plywood.
* Vadim Ogievetsky talks about the [inspiration behind Plywood](https://www.youtube.com/watch?v=JNMbLxqzGFA)

## Questions & Support

For updates about new and upcoming features follow [@implydata](https://twitter.com/implydata) on Twitter.

Please file bugs and feature requests by opening and issue on GitHub and direct all questions to our [user groups](https://groups.google.com/forum/#!forum/imply-user-group).
