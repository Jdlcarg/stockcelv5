
import { createClient } from 'redis';

class CacheManager {
  private client: any;
  private isConnected: boolean = false;

  constructor() {
    this.client = createClient({
      url: process.env.REDIS_URL || 'redis://localhost:6379',
      retry_strategy: (options) => {
        if (options.error && options.error.code === 'ECONNREFUSED') {
          console.warn('Redis connection refused, running without cache');
          return null;
        }
        if (options.total_retry_time > 1000 * 60 * 60) {
          return null;
        }
        return Math.min(options.attempt * 100, 3000);
      }
    });

    this.client.on('error', (err: Error) => {
      console.warn('Redis Client Error:', err.message);
      this.isConnected = false;
    });

    this.client.on('connect', () => {
      console.log('✅ Redis Cache Connected');
      this.isConnected = true;
    });

    this.connect();
  }

  private async connect() {
    try {
      await this.client.connect();
    } catch (error) {
      console.warn('Redis not available, running without cache:', error.message);
    }
  }

  async get(key: string): Promise<any> {
    if (!this.isConnected) return null;
    
    try {
      const value = await this.client.get(key);
      return value ? JSON.parse(value) : null;
    } catch (error) {
      console.warn('Cache get error:', error.message);
      return null;
    }
  }

  async set(key: string, value: any, ttl: number = 300): Promise<void> {
    if (!this.isConnected) return;
    
    try {
      await this.client.setEx(key, ttl, JSON.stringify(value));
    } catch (error) {
      console.warn('Cache set error:', error.message);
    }
  }

  async del(key: string): Promise<void> {
    if (!this.isConnected) return;
    
    try {
      await this.client.del(key);
    } catch (error) {
      console.warn('Cache delete error:', error.message);
    }
  }

  async invalidatePattern(pattern: string): Promise<void> {
    if (!this.isConnected) return;
    
    try {
      const keys = await this.client.keys(pattern);
      if (keys.length > 0) {
        await this.client.del(keys);
      }
    } catch (error) {
      console.warn('Cache invalidate error:', error.message);
    }
  }

  generateKey(prefix: string, ...params: (string | number)[]): string {
    return `${prefix}:${params.join(':')}`;
  }
}

export const cache = new CacheManager();
