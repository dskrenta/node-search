'use strict';
const crypto = require('crypto');
const nlp = require('compromise');
const stopwords = require('stopwords').english;

class NodeSearch {
  constructor() {
    this.invertedIndex = {};
    this.documentStore = {};
    this.fields = {};
    this.fieldsCount = 0;
    this.fieldBoosts = [];
  }

  addDocument(document) {
    const documentId = crypto.createHash('md5').update(JSON.stringify(document)).digest('hex');
    this.documentStore[documentId] = document;
    this.indexDocument(document, documentId);
  }

  addFieldBoosts(fieldBoosts) {
    for (let field in fieldBoosts) {
      this.fieldBoosts[this.fields[field]] = fieldBoosts[field];
    }
  }

  indexDocument(document, documentId) {
    const tokenizedDocument = this.tokenizeFields(document);
    // stem verbs
    for (let field in tokenizedDocument) {
      for (let token of tokenizedDocument[field]) {
        if (!stopwords.includes(token)) {
          const posting = [
            this.termFrequency(token, tokenizedDocument[field]),
            this.inverseDocumentFrequency(token),
            this.fieldLengthNormalization(tokenizedDocument[field]),
            this.fields[field]
          ];
          if (token in this.invertedIndex) {
            if (documentId in this.invertedIndex[token]) {
              this.invertedIndex[token][documentId].push(posting);
            }
            else {
              this.invertedIndex[token][documentId] = [posting];
            }
          }
          else {
            this.invertedIndex[token] = {[documentId]: [posting]};
          }
        }
      }
    }
  }

  scoreDocuments(tokenizedQuery) {
    const scores = {};
    for (let token of tokenizedQuery) {
      for (let documentId in this.invertedIndex[token]) {
        if (documentId in scores) {
          scores[documentId] += this.termScore(this.invertedIndex[token][documentId]);
        }
        else {
          scores[documentId] = this.termScore(this.invertedIndex[token][documentId]);
        }
      }
    }
    return scores;
  }

  termScore(postings) {
    let score = 0;
    for (let posting of postings) {
      score += posting[0] * posting[1] * posting[2] * this.fieldBoosts[posting[3]];
    }
    return score;
  }

  search(query) {
    const tokenizedQuery = this.tokenizeNormalize(query);
    const queryIdfs = tokenizedQuery.map(token => this.inverseDocumentFrequency(token));
    const results = this.scoreDocuments(tokenizedQuery);
    for (let documentId in results) {
      results[documentId] *= this.queryNormalization(queryIdfs) * this.queryCoordination(tokenizedQuery, documentId);
    }
    const sortedResults = Object.keys(results).sort((a, b) => {
      if (results[a] < results[b]) {
        return 1;
      }
      if (results[a] > results[b]) {
        return -1;
      }
      return 0;
    });
    return sortedResults.map(key => this.documentStore[key]);
  }

  tokenizeFields(document) {
    const tokenizedDocument = {};
    for (let field in document) {
      if (!(field in this.fields)) {
        this.fields[field] = this.fieldsCount;
        if (!this.fieldBoosts[this.fieldsCount]) this.fieldBoosts[this.fieldsCount] = 1;
        this.fieldsCount++;
      }
      if (typeof document[field] == 'string') {
        tokenizedDocument[field] = this.tokenizeNormalize(document[field]);
      }
    }
    return tokenizedDocument;
  }

  tokenizeNormalize(string) {
    const parsed = nlp(string);
    return parsed.terms().data().map(data => data.normal);
  }

  termFrequency(term, tokenizedDocument) {
    const frequency = tokenizedDocument.filter(token => token === term).length;
    return Math.sqrt(frequency);
  }

  inverseDocumentFrequency(term) {
    const numDocuments = Object.keys(this.documentStore).length;
    const numDocumentsContainTerm = term in this.invertedIndex ? Object.keys(this.invertedIndex[term]).length : 0;
    return 1 + Math.log(numDocuments / (numDocumentsContainTerm + 1));
  }

  fieldLengthNormalization(tokenizedDocument) {
    return 1 / Math.sqrt(tokenizedDocument.length);
  }

  queryNormalization(queryIdfs) {
    const sumOfSquaredWeights = queryIdfs.reduce((total, idf) => total + Math.sqrt(idf, 2));
    return 1 / Math.sqrt(sumOfSquaredWeights);
  }

  queryCoordination(tokenizedQuery, documentId) {
    let matchingTerms = 0;
    for (let token of tokenizedQuery) {
      if (token in this.invertedIndex) { 
        if (documentId in this.invertedIndex[token]) {
          matchingTerms++;
        }
      }
    }
    return matchingTerms / tokenizedQuery.length;
  }

  log() {
    console.log(`Inverted Index: ${JSON.stringify(this.invertedIndex, null, 2)}`);
    console.log(`Document Store: ${JSON.stringify(this.documentStore, null, 2)}`);
    console.log(`Fields: ${JSON.stringify(this.fields, null, 2)}`);
  }
}

module.exports = NodeSearch;
