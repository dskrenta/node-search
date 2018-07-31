'use strict';

const level = require('level');
const nlp = require('compromise');
const stopwords = require('stopwords').english;
const crypto = require('crypto');

const INVERTED_INDEX_KEY_PREFIX = 'InvertedIndex';
const DOCUMENT_STORE_KEY_PREFIX =  'DocumentStore';
const FIELDS_KEY_PREFIX = 'Fields';
const FIELDS_COUNT_KEY = 'FieldsCount';
const FIELDS_BOOST_KEY = 'FieldsBoosts';

class NodeSearch {
  constructor(storePath) {
    this.store = level(storePath);
  }

  // Public 

  async add(document) {
    try {
      const documentId = this._generateDocumentId(document);
      await this.store.put(`${DOCUMENT_STORE_KEY_PREFIX}:${documentId}`, JSON.stringify(document));
      this._indexDocument(documentId, document);
    }
    catch(error) {
      console.error(error);
    }
  }

  async update(documentId, partialUpdatedDocument) {
    try {
      const previousDocumentVersion = await this.get(documentId);
      const newDocument = Object.assign({}, previousDocumentVersion, partialUpdatedDocument);
      await this.store.put(`${DOCUMENT_STORE_KEY_PREFIX}:${documentId}`, JSON.stringify(newDocument));
    }
    catch (error) {
      console.error(error);
    }
  }

  async delete(documentId) {
    try {
      await this.store.del(`${DOCUMENT_STORE_KEY_PREFIX}:${documentId}`);
    }
    catch (error) {
      console.error(error);
    } 
  }

  async get(documentId) {
    try {
      return await this.store.get(`${DOCUMENT_STORE_KEY_PREFIX}:${documentId}`);
    }
    catch (error) {
      if (error.notFound) {
        return null;
      }
      console.error(error);
    } 
  }

  search() {
    return null;
  }

  log() {
    this.store.createReadStream()
      .on('data', (data) => {
        console.log(`${data.key}: ${data.value}`);
      })
      .on('error', function (err) {
        console.log('Error', err)
      })
      .on('end', function () {
        console.log('Stream ended')
      });
  }

  addFieldBoosts() {
    return null;
  }

  // Private

  _generateDocumentId(document) {
    return crypto.createHash('md5').update(JSON.stringify(document)).digest('hex');
  }

  _indexDocument(documentId, document) {
    return null;
  }

  _tokenizeFields(document) {
    const tokenizedDocument = {};
    for (let field in document) {
      // check if field exists
      if (!(field in this.fields)) {
        // update field with fieldCount id
        this.fields[field] = this.fieldsCount;
        // if boost does not exist set to 1
        if (!this.fieldBoosts[this.fieldsCount]) this.fieldBoosts[this.fieldsCount] = 1;
        // update fields count
        this.fieldsCount++;
      }
      if (typeof document[field] == 'string') {
        tokenizedDocument[field] = this.tokenizeNormalize(document[field]);
      }
    }
    return tokenizedDocument;
  }

  async _getFieldsCount() {
    try {
      return await this.store.get(FIELDS_COUNT_KEY);
    }
    catch (error) {
      console.error(error);
    }
  }

  async _setFieldsCount(count) {
    try {
      await this.store.put(FIELDS_COUNT_KEY, count);
    }
    catch(error) {
      console.error(error);
    }
  }

  async _getFieldBoosts() {
    try {
      const rawFieldBoosts = await this.store.get(FIELDS_BOOST_KEY);
      return rawFieldBoosts.split(',');
    }
    catch (error) {
      console.error(error);
    }
  }

  async _setFieldBoosts(boosts) {
    try {
      await this.store.put(FIELDS_BOOST_KEY, boosts.join(','));
    }
    catch (error) {
      console.error(error);
    }
  } 

  async _getField(field) {
    try {
      return await this.store.get(`${FIELDS_KEY_PREFIX}:${field}`);
    }
    catch (error) {
      console.error(error);
    }
  }

  async _setField(field, count) {
    try {
      await this.store.put(`${FIELDS_KEY_PREFIX}:${field}`, count);
    }
    catch (error) {
      console.error(error);
    }
  }
}

module.exports = NodeSearch;