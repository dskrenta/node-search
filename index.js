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
const DOCUMENT_COUNT_KEY = 'DocumentCount';

class NodeSearch {
  constructor(storePath) {
    this.store = level(storePath);
  }

  // Public 

  async add(document) {
    try {
      const documentId = this._generateDocumentId(document);
      await this.store.put(`${DOCUMENT_STORE_KEY_PREFIX}:${documentId}`, JSON.stringify(document));
      await this._incrementNumDocuments(1);
      await this._indexDocument(documentId, document);
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
        console.log('\n');
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

  async _indexDocument(documentId, document) {
    try {
      const tokenizedDocument = this._tokenizeFields(document);
      for (let field in tokenizedDocument) {
        for (let token of tokenizedDocument[field]) {
          if (!stopwords.includes(token)) {
            const idf = await this._inverseDocumentFrequency(token);
            const posting = [
              this._termFrequency(token, tokenizedDocument[field]),
              idf,
              this._fieldLengthNormalization(tokenizedDocument[field])
            ];
            this._invertedIndexInsert(token, documentId, posting);
          }
        }
      }
    }
    catch(error) {
      console.error(error);
    }
  }

  async _invertedIndexInsert(token, documentId, posting) {
    try {
      const record = await this._invertedIndexGetToken(token);
      if (record) {
        const parsed = JSON.parse(record);
        if (documentId in parsed) {
          parsed[documentId].push(posting);
        } 
        else { 
          parsed[documentId] = [posting];
        }
        await this._invertedIndexSetToken(token, JSON.stringify(parsed));
      }
      else {
        const createdRecord = {[documentId]: [posting]};
        await this._invertedIndexSetToken(token, JSON.stringify(createdRecord));
      }
    }
    catch(error) {
      console.error(error);
    }
  }

  async _invertedIndexGetToken(token) {
    try {
      return await this.store.get(`${INVERTED_INDEX_KEY_PREFIX}:${token}`);
    }
    catch (error) {
      if (error.notFound) {
        return null;
      }
      console.error(error);
    }
  }

  async _invertedIndexSetToken(token, record) {
    try {
      await this.store.put(`${INVERTED_INDEX_KEY_PREFIX}:${token}`, record);
    }
    catch (error) {
      console.error(error);
    }
  }

  _termFrequency(term, tokenizedDocument) {
    const frequency = tokenizedDocument.filter(token => token === term).length;
    return Math.sqrt(frequency);
  }

  async _inverseDocumentFrequency(term) {
    try {
      const numDocuments = await this._getNumDocuments();
      const documentsContainTermRaw = await this._invertedIndexGetToken(term);
      const documentsContainTerm = JSON.parse(documentsContainTermRaw);
      const numDocumentsContainTerm = documentsContainTerm ? documentsContainTerm.length : 0;
      return 1 + Math.log(numDocuments / (numDocumentsContainTerm + 1));  
    }
    catch(error) {
      console.error(error);
    }
  }

  _fieldLengthNormalization(tokenizedDocument) {
    return 1 / Math.sqrt(tokenizedDocument.length);
  }

  _tokenizeFields(document) {
    const tokenizedDocument = {};
    for (let field in document) {
      if (typeof document[field] === 'string') {
        tokenizedDocument[field] = this._tokenizeNormalize(document[field]);
      }
    }
    return tokenizedDocument;
  }

  _tokenizeNormalize(string) {
    const parsed = nlp(string);
    return parsed.terms().data().map(data => data.normal);
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
    catch (error) {
      console.error(error);
    }
  }

  async _getFieldBoosts() {
    try {
      const rawFieldBoosts = await this.store.get(FIELDS_BOOST_KEY);
      return rawFieldBoosts.split(',');
    }
    catch (error) {
      if (error.notFound) {
        return null;
      }
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
      if (error.notFound) {
        return null;
      }
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

  async _getNumDocuments() {
    try {
      return await this.store.get(DOCUMENT_COUNT_KEY);
    }
    catch (error) {
      if (error.notFound) {
        return null;
      }
      console.error(error);
    }
  }

  async _setNumDocuments(field, num) {
    try {
      await this.store.put(DOCUMENT_COUNT_KEY, num);
    }
    catch (error) {
      console.error(error);
    }
  }

  async _incrementNumDocuments(num = 1) {
    try {
      const storedDocumentCount = await this._getNumDocuments();
      const documentCount = storedDocumentCount || 0;
      await this._setNumDocuments(documentCount + num);
    }
    catch (error) {
      console.error(error);
    }
  }
}

module.exports = NodeSearch;