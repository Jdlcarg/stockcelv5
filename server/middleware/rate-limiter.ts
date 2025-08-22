
import rateLimit from 'express-rate-limit';

// General API rate limit
export const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 1000, // Limit each IP to 1000 requests per windowMs
  message: {
    error: 'Too many requests from this IP, please try again later.',
    retryAfter: 15 * 60 // 15 minutes
  },
  standardHeaders: true,
  legacyHeaders: false,
  skip: (req) => {
    // Skip rate limiting for localhost in development
    return process.env.NODE_ENV === 'development' && 
           (req.ip === '127.0.0.1' || req.ip === '::1');
  }
});

// Strict rate limit for authentication endpoints
export const authLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 10, // Limit each IP to 10 auth requests per windowMs
  message: {
    error: 'Too many authentication attempts, please try again later.',
    retryAfter: 15 * 60
  },
  standardHeaders: true,
  legacyHeaders: false,
  skipSuccessfulRequests: true,
});

// Very strict rate limit for password reset
export const passwordResetLimiter = rateLimit({
  windowMs: 60 * 60 * 1000, // 1 hour
  max: 3, // Limit each IP to 3 password reset requests per hour
  message: {
    error: 'Too many password reset attempts, please try again later.',
    retryAfter: 60 * 60
  },
  standardHeaders: true,
  legacyHeaders: false,
});

// Rate limit for product scanning
export const scanLimiter = rateLimit({
  windowMs: 1 * 60 * 1000, // 1 minute
  max: 100, // Limit each IP to 100 scans per minute
  message: {
    error: 'Too many scan requests, please slow down.',
    retryAfter: 60
  },
  standardHeaders: true,
  legacyHeaders: false,
});
