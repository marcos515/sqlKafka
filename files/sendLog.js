const axios = require("axios")
module.exports = async (message) => {
    var config = require("./config.json")
    sendMessage(config, message)
}
async function sendMessage(config, message) {
    try {
        await axios.get(`https://api.telegram.org/bot${config.botToken}/sendMessage?chat_id=${config.chatId}&text=${encodeURIComponent(message)}`)
        return 'ok'
    } catch (error) {
        return "err"
    }


}