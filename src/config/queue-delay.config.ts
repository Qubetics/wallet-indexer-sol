// Queue Delay Configuration for Solana RPC Load Management
export const QUEUE_DELAY_CONFIG = {
    // Base delay settings
    BASE_DELAY: 100,           // 100ms base delay between queue operations
    MAX_DELAY: 2000,           // 2 seconds maximum delay
    MIN_DELAY: 50,             // 50ms minimum delay

    // RPC rate limiting
    RPC_RATE_LIMIT: 50,        // RPC calls per second limit
    RATE_LIMIT_BUFFER: 1.2,    // 20% buffer for rate limiting

    // Volume-based delays
    VOLUME_THRESHOLDS: {
        HIGH: 100,              // High volume threshold
        MEDIUM: 50,             // Medium volume threshold
        LOW: 10                 // Low volume threshold
    },

    // Delay multipliers for different volumes
    DELAY_MULTIPLIERS: {
        HIGH: 3,                // 3x delay for high volume
        MEDIUM: 2,              // 2x delay for medium volume
        LOW: 1.5                // 1.5x delay for low volume
    },

    // Jitter settings
    JITTER_MAX: 200,           // Maximum random jitter in ms

    // Error handling
    MAX_RETRIES: 3,            // Maximum retry attempts
    BASE_RETRY_DELAY: 1000,    // Base retry delay in ms

    // Performance monitoring
    SLOW_QUEUE_THRESHOLD: 100, // Log warning if queue operation takes longer than this (ms)

    // Block processing delays
    BLOCK_PROCESSING: {
        LARGE_BLOCK_DELAY: 500,    // Delay for blocks with >1000 transactions
        MEDIUM_BLOCK_DELAY: 200,   // Delay for blocks with 100-1000 transactions
        SMALL_BLOCK_DELAY: 50      // Delay for blocks with <100 transactions
    }
};

// Environment-specific overrides
export const getQueueDelayConfig = () => {
    const env = process.env.NODE_ENV || 'development';

    switch (env) {
        case 'production':
            return {
                ...QUEUE_DELAY_CONFIG,
                BASE_DELAY: 150,        // Slightly higher delay in production
                MAX_DELAY: 3000,        // Higher max delay for stability
                RPC_RATE_LIMIT: 40      // More conservative rate limiting
            };
        case 'staging':
            return {
                ...QUEUE_DELAY_CONFIG,
                BASE_DELAY: 120,
                MAX_DELAY: 2500
            };
        default:
            return QUEUE_DELAY_CONFIG;
    }
}; 