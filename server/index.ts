import express, { type Request, Response, NextFunction } from "express";
import { registerRoutes } from "./routes";
import { setupVite, serveStatic, log } from "./vite";
import { startAutoSyncMonitor } from "./auto-sync-monitor.js";

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

app.use((req, res, next) => {
  const start = Date.now();
  const path = req.path;
  let capturedJsonResponse: Record<string, any> | undefined = undefined;

  const originalResJson = res.json;
  res.json = function (bodyJson, ...args) {
    capturedJsonResponse = bodyJson;
    return originalResJson.apply(res, [bodyJson, ...args]);
  };

  res.on("finish", () => {
    const duration = Date.now() - start;
    if (path.startsWith("/api")) {
      let logLine = `${req.method} ${path} ${res.statusCode} in ${duration}ms`;
      if (capturedJsonResponse) {
        logLine += ` :: ${JSON.stringify(capturedJsonResponse)}`;
      }

      if (logLine.length > 80) {
        logLine = logLine.slice(0, 79) + "…";
      }

      log(logLine);
    }
  });

  next();
});

(async () => {
  const server = await registerRoutes(app);

  app.use((err: any, _req: Request, res: Response, _next: NextFunction) => {
    const status = err.status || err.statusCode || 500;
    const message = err.message || "Internal Server Error";

    res.status(status).json({ message });
    throw err;
  });

  // importantly only setup vite in development and after
  // setting up all the other routes so the catch-all route
  // doesn't interfere with the other routes
  if (app.get("env") === "development") {
    await setupVite(app, server);
  } else {
    serveStatic(app);
  }

  // ALWAYS serve the app on the port specified in the environment variable PORT
  // Other ports are firewalled. Default to 5000 if not specified.
  // this serves both the API and the client.
  // It is the only port that is not firewalled.
  const port = parseInt(process.env.PORT || '5000', 10);
  server.listen({
    port,
    host: "0.0.0.0",
    reusePort: true,
  }, () => {
    log(`🚀 Server running on port ${port}`);
    log(`📱 Dashboard: http://localhost:${port}`);

    // Inicializar servicio de automatización de caja
    (async () => {
      try {
        const { cashAutomationService } = await import('./cash-automation-service.js');

        // Initialize cash automation service
        if (process.env.NODE_ENV !== 'test') {
          console.log('🚀 Starting cash automation service...');
          cashAutomationService.start();
          console.log('🤖 Cash automation service started successfully');

          // Verify service is running after a short delay
          setTimeout(() => {
            console.log('🔍 Verifying cash automation service status...');
            const status = cashAutomationService.getStatus();
            if (status.isRunning) {
              console.log('✅ Cash automation service is confirmed running');
            } else {
              console.error('❌ Cash automation service failed to start');
            }
          }, 2000);
        }

        // Verificar status cada 30 segundos y reiniciar si se detiene
        setInterval(() => {
          const status = cashAutomationService.getStatus();
          if (!status.isRunning) {
            console.log('⚠️ Cash automation service stopped, restarting...');
            cashAutomationService.start();
          }
        }, 30000);

      } catch (error) {
        console.error('❌ Error initializing cash automation service:', error);
      }
    })();
  });

  // Start automatic synchronization monitor after server starts
  setTimeout(() => {
    console.log('🚀 [AUTO-SYNC] Iniciando monitor de sincronización automática...');
    startAutoSyncMonitor();
  }, 3000); // Wait 3 seconds for server to be fully ready
})();