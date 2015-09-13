{ expect } = require("chai")

plywood = require('../../build/plywood')
{ Expression, Dataset, ply, $, r, mark } = plywood

describe "render", ->

  context = {
    wiki: Dataset.fromJS([
      { language: 'en', page: 'Hello' }
    ])
  }

  describe "bind", ->
    it "fails to resolve a variable that does not exist", ->
      ex = ply()
        .apply("wiki", $('wiki').filter("$language == 'en'"))
        .apply('cont', mark('div.vis', {
          style: { width: '100px' }
        }))
        .apply('Pages',
          $('wiki').split('$page', 'Page')
            .apply('Count', '$wiki.count()')
            .sort('$Count', 'descending')
            .limit(10)
            .apply('page-cont',
              $('cont').attach('div.page', {
                style: {
                  width: '$Count'
                }
              })
            )
            .apply('label',
              $('page-cont').attach('div.label', {
                text: '$Page'
              })
            )
        )

      ex = ex.referenceCheck(context);

      expect(ex.getBindSpecs()).to.deep.equal([
        {
          "selectionInput": "__base__"
          "selectionName": "cont"
          "selector": "div.vis"
          "style": ["width"]
        }
        {
          selectionInput: 'cont'
          selector: 'div.page'
          selectionName: 'page-cont'
          data: 'Pages'
          key: 'Page'
          style: ['width']
        }
        {
          selectionInput: 'page-cont'
          selector: 'div.label'
          selectionName: 'label'
          text: true
        }
      ])
