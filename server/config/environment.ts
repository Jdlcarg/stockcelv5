
import { z } from 'zod';

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  PORT: z.string().transform(Number).default('5000'),
  DATABASE_URL: z.string().url('Invalid database URL'),
  JWT_SECRET: z.string().min(32, 'JWT secret must be at least 32 characters'),
  REDIS_URL: z.string().url().optional(),
  EMAIL_SERVICE: z.enum(['sendgrid', 'gmail', 'smtp']).default('sendgrid'),
  SENDGRID_API_KEY: z.string().optional(),
  SMTP_HOST: z.string().optional(),
  SMTP_PORT: z.string().transform(Number).optional(),
  SMTP_USER: z.string().optional(),
  SMTP_PASS: z.string().optional(),
  RATE_LIMIT_WINDOW_MS: z.string().transform(Number).default('900000'), // 15 minutes
  RATE_LIMIT_MAX_REQUESTS: z.string().transform(Number).default('1000'),
  BCRYPT_SALT_ROUNDS: z.string().transform(Number).default('14'),
  SESSION_SECRET: z.string().min(32, 'Session secret must be at least 32 characters'),
  CORS_ORIGIN: z.string().default('*'),
  LOG_LEVEL: z.enum(['error', 'warn', 'info', 'debug']).default('info'),
});

export type Environment = z.infer<typeof envSchema>;

let env: Environment;

try {
  env = envSchema.parse(process.env);
} catch (error) {
  console.error('❌ Invalid environment configuration:');
  if (error instanceof z.ZodError) {
    error.errors.forEach((err) => {
      console.error(`  - ${err.path.join('.')}: ${err.message}`);
    });
  }
  process.exit(1);
}

export { env };

// Helper functions
export const isDevelopment = () => env.NODE_ENV === 'development';
export const isProduction = () => env.NODE_ENV === 'production';
export const isTest = () => env.NODE_ENV === 'test';
