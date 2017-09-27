class MyError extends Error {
	constructor(message, { error = null , object = null } = {} ) {
		super(message);
		this.error = error;
		this.object = object;
		if (this.error) {
			this.code = this.error.code;
		}
	}

	toString() {
		return this.message;
	}
}

const arangoErrors = {
	ERROR_HTTP_NOT_FOUND: { code: 404 },
	ERROR_ARANGO_DOCUMENT_NOT_FOUND: { code: 1202 },
	ERROR_ARANGO_CONFLICT: { code: 1200 },
	ERROR_ARANGO_UNIQUE_CONSTRAINT_VIOLATED: { code: 1210 },
	ERROR_ARANGO_GRAPH_NOT_FOUND: { code: 1924 }
};

module.exports = {
	MyError,
arangoErrors};
