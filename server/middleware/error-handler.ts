
import type { Request, Response, NextFunction } from "express";

export interface AppError extends Error {
  statusCode?: number;
  isOperational?: boolean;
}

export const createError = (message: string, statusCode: number = 500): AppError => {
  const error: AppError = new Error(message);
  error.statusCode = statusCode;
  error.isOperational = true;
  return error;
};

export const errorHandler = (
  error: AppError,
  req: Request,
  res: Response,
  next: NextFunction
) => {
  // Default error values
  let statusCode = error.statusCode || 500;
  let message = error.message || 'Internal Server Error';
  
  // Log error for debugging
  console.error('Error caught by error handler:', {
    message: error.message,
    stack: error.stack,
    url: req.url,
    method: req.method,
    timestamp: new Date().toISOString()
  });

  // Handle specific error types
  if (error.name === 'ValidationError') {
    statusCode = 400;
    message = 'Validation error: ' + error.message;
  }
  
  if (error.name === 'UnauthorizedError') {
    statusCode = 401;
    message = 'Unauthorized access';
  }

  if (error.message.includes('duplicate key')) {
    statusCode = 409;
    message = 'Resource already exists';
  }

  if (error.message.includes('not found')) {
    statusCode = 404;
    message = 'Resource not found';
  }

  // Don't expose internal errors in production
  if (process.env.NODE_ENV === 'production' && !error.isOperational) {
    message = 'Something went wrong';
  }

  res.status(statusCode).json({
    success: false,
    message,
    ...(process.env.NODE_ENV === 'development' && { stack: error.stack })
  });
};

export const asyncHandler = (fn: Function) => {
  return (req: Request, res: Response, next: NextFunction) => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
};
