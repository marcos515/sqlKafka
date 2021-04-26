const yargs = require('yargs');
const fs = require("fs")
setInterval(()=>{
    var args = yargs.argv
    fs.writeFileSync('./trash.txt', fs.readFileSync('./trash.txt', 'utf8') + "\n"+ args.msg)
}, 4000)