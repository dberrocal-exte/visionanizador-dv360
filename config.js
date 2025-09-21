// config.js
export default {
  paths: {
    raw: './rawData',
    intermediate: './intermediate',
    processed: './processed',
    dictionary: './dictionary'
  },
  csv: {
    delimiter: ',', // allow override if a source uses ';'
    encoding: 'utf8'
  },
  mapping: {
    categories: {
      // index positions in the source CSV (0-based)
      insertionOrder: 0,
      date: 1,
      category: 2,
      appUrl: 3,
      impressions: 4,
      clicks: 5,
      viewableImpressions: 6
    },
    genders: {
      insertionOrder: 0,
      date: 1,
      gender: 2,
      age: 3,
      impressions: 4,
      clicks: 5
    },
    device: {
      insertionOrder: 0,
      date: 1,
      deviceType: 2,
      impressions: 3,
      clicks: 4,
      viewableImpressions: 5
    }
  }
}
