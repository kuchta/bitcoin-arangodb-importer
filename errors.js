class MyError extends Error {
	constructor(message, { error = null , object = null } = {} ) {
		super(message);
		this.error = error;
		this.object = object;
		if (this.error) {
			if (this.error.isArangoError) {
				this.code = this.error.errorNum;
			} else {
				this.code = this.error.code;
			}
		}
	}

	toString() {
		let msg = this.message;
		if (this.code) {
			msg = `${msg} (code: ${this.code})`
		}
		return msg;
	}
}

const arangoErrors = {
	ERROR_HTTP_NOT_FOUND: { code: 404 },
	ERROR_ARANGO_CONFLICT: { code: 1200 },
	ERROR_ARANGO_DOCUMENT_NOT_FOUND: { code: 1202 },
	ERROR_ARANGO_COLLECTION_NOT_FOUND: { code: 1203 },
	ERROR_ARANGO_UNIQUE_CONSTRAINT_VIOLATED: { code: 1210 },
	ERROR_ARANGO_GRAPH_NOT_FOUND: { code: 1924 }
};

module.exports = {
	MyError,
	arangoErrors
};
