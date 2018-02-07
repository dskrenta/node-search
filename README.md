# node-search
Node.js persistent full text search engine 

## Implementation Notes
```
const index = new NodeSearch();

const documents = [
    {
        id: 1,
        title: 'Great cooking ideas'
    },
    {
        id: 2,
        title: 'Interesting vacation destinations'
    },
    {
        id: 3,
        title: 'Sample document'
    }
];

for (document of documents) {
    index.addDocument(document)
}

index.get(1);

index.search('Vacation cooking');
```