class MyError extends Error {
	constructor (message, {error = null, object = null} = {}) {
		super(message);
		this.error = error;
		this.object = object;
	}

	toString () {
		return this.message;
	}
}

const arangoErrors = {
	ERROR_ARANGO_CONFLICT: { code: 1200 },
	ERROR_ARANGO_UNIQUE_CONSTRAINT_VIOLATED: { code: 1210 }
};

module.exports = {
	MyError,
	arangoErrors};
