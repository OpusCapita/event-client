const CustomError = require('./CustomError');

class NotImplError extends CustomError
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

module.exports = NotImplError;

