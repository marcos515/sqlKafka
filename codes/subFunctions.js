
function parseInts(n) {
    try {
        return parseInt(n)
    } catch (error) {
        return n
    }
}




module.exports = { parseInts }