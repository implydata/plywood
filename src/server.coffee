express = require('express')

app = express()

app.disable('x-powered-by')

app.use(express.static(__dirname + '/../inspector'))
app.use(express.static(__dirname + '/../package'))

#app.get '/', (req, res) ->
#  res.send('Welcome to Plywood')
#  return

app.listen(9876)
console['log']('Listening on port 9876')
