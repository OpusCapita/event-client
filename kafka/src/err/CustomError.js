class EventError extends Error
{
    /**
     * @param {string} message
     * @param {string} code - The string error code
     * @param {number} errno - Error number
     */
    constructor(message, code, errno)
    {
        super(message);
        Error.captureStackTrace(this, EventError);

        this.code = code;
        this.errno = errno;
    }
}

module.exports = EventError;
