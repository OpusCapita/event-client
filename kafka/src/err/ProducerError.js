const CustomError = require('./CustomError');

class ProducerError extends CustomError
{
    /**
     * @param {string} message
     * @param {string} code - The string error code
     * @param {number} errno - Error number
     */
    constructor(...args)
    {
        super(...args);
    }
}

module.exports = ProducerError;
