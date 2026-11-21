export const MINIMUM_BID = 0.01;

export function meetsMinimumBid(value) {
  const decimal = String(value ?? '').trim();
  return /^\d+(\.\d+)?$/.test(decimal) && Number.isFinite(Number(decimal)) && Number(decimal) >= MINIMUM_BID;
}

function wholeNumber(value, fallback) {
  const n = Number(value);
  return Number.isSafeInteger(n) ? n : fallback;
}

// Number inputs in the Rancher form emit strings. The driver schema and Spot
// pool JSON expect integers, so coerce them before the cluster is saved.
export function normalizeManualSpotConfig(config, additionalPools) {
  return {
    config: {
      ...config,
      spotNodeCount: wholeNumber(config.spotNodeCount, 0),
      spotAutoscalingMinNodes: wholeNumber(config.spotAutoscalingMinNodes, 0),
      spotAutoscalingMaxNodes: wholeNumber(config.spotAutoscalingMaxNodes, 0),
      onDemandNodeCount: wholeNumber(config.onDemandNodeCount, 0),
    },
    additionalSpotPools: (additionalPools || []).map(pool => ({
      ...pool,
      nodeCount: wholeNumber(pool.nodeCount, 0),
      minNodes: wholeNumber(pool.minNodes, 0),
      maxNodes: wholeNumber(pool.maxNodes, 0),
    })),
  };
}

export function validateSpotBids(config, additionalPools) {
  if (!config.disablePrimarySpotPool && !meetsMinimumBid(config.spotBidPrice)) {
    return 'Primary spot pool bid must be at least $0.01 per hour.';
  }
  for (const [index, pool] of additionalPools.entries()) {
    if (!meetsMinimumBid(pool.bidPrice)) return `Spot pool ${index + 2} bid must be at least $0.01 per hour.`;
  }
  return '';
}
