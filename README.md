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
nested data structure so they can be easily consumed by visualization libraries
such as [D3.js](http://d3js.org/). 

You can use Plywood in the browser and/or in node.js to easily create your own
visualizations and applications.

Plywood also acts as a very advanced query planner for Druid, and Plywood will
determine the most optimal way to execute Druid queries.

## Installation

To use Plywood from npm simply run: `npm install plywood`.

Plywood can also be used by the browser.

## Documentation

To learn more, see [http://plywood.imply.io](http://plywood.imply.io/)

## Questions & Support

For updates about new and upcoming features follow [@implydata](https://twitter.com/implydata) on Twitter.

Please file bugs and feature requests by opening and issue on GitHub and direct all questions to our [user groups](https://groups.google.com/forum/#!forum/imply-user-group).
