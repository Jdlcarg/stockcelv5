
import { env } from '../config/environment';

export enum LogLevel {
  ERROR = 0,
  WARN = 1,
  INFO = 2,
  DEBUG = 3
}

const logLevelMap: Record<string, LogLevel> = {
  error: LogLevel.ERROR,
  warn: LogLevel.WARN,
  info: LogLevel.INFO,
  debug: LogLevel.DEBUG
};

class Logger {
  private currentLevel: LogLevel;

  constructor() {
    this.currentLevel = logLevelMap[env.LOG_LEVEL] ?? LogLevel.INFO;
  }

  private shouldLog(level: LogLevel): boolean {
    return level <= this.currentLevel;
  }

  private formatMessage(level: string, message: string, meta?: any): string {
    const timestamp = new Date().toISOString();
    const logEntry = {
      timestamp,
      level,
      message,
      ...(meta && { meta })
    };
    
    return JSON.stringify(logEntry);
  }

  error(message: string, meta?: any): void {
    if (this.shouldLog(LogLevel.ERROR)) {
      console.error(this.formatMessage('ERROR', message, meta));
    }
  }

  warn(message: string, meta?: any): void {
    if (this.shouldLog(LogLevel.WARN)) {
      console.warn(this.formatMessage('WARN', message, meta));
    }
  }

  info(message: string, meta?: any): void {
    if (this.shouldLog(LogLevel.INFO)) {
      console.info(this.formatMessage('INFO', message, meta));
    }
  }

  debug(message: string, meta?: any): void {
    if (this.shouldLog(LogLevel.DEBUG)) {
      console.debug(this.formatMessage('DEBUG', message, meta));
    }
  }

  // Specific logging methods for common use cases
  apiRequest(method: string, url: string, statusCode: number, duration: number, userId?: number): void {
    this.info('API Request', {
      method,
      url,
      statusCode,
      duration,
      userId
    });
  }

  databaseQuery(query: string, duration: number, error?: Error): void {
    if (error) {
      this.error('Database Query Failed', { query, duration, error: error.message });
    } else {
      this.debug('Database Query', { query, duration });
    }
  }

  authEvent(event: string, userId?: number, email?: string, success: boolean = true): void {
    this.info('Auth Event', {
      event,
      userId,
      email,
      success
    });
  }
}

export const logger = new Logger();
