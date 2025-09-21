// utils/argumentos.js
// Reusable yargs option descriptors

export const categoryTier = {
  name: 'tiers',
  config: {
    type: 'number',
    default: 1,
    choices: [1, 2, 3, 4],
    describe: 'Max depth of tiers to extract'
  }
};

export const dataProvider = {
  name: 'provider',
  config: {
    type: 'string',
    default: 'dv',
    choices: ['dv', 'ttd', 'zed'],
    describe: 'Source provider behavior'
  }
};

// Helper to apply common options to a yargs instance
export function applyCommonArgs(y) {
  return y
    .option(categoryTier.name, categoryTier.config)
    .option(dataProvider.name, dataProvider.config);
}
