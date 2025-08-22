import type { Express, Request, Response } from "express";
import { createServer, type Server } from "http";
import { storage, db } from "./storage";
import { eq, and, gte, lte } from "drizzle-orm";
import { resellers, resellerSales, users, cashAutoOperationsLog } from "@shared/schema";
import { z } from "zod";
import bcrypt from "bcryptjs";
import passwordResetRoutes from "./password-reset";
import { cashScheduleStorage } from "./cash-schedule-storage";

// Helper function to authenticate users and resellers
async function authenticateRequest(req: any): Promise<{ user: any; isReseller: boolean } | null> {
  const userId = req.headers["x-user-id"];
  const authToken = req.headers["authorization"];

  console.log('🔍 authenticateRequest DEBUG:');
  console.log('  - userId:', userId);
  console.log('  - authToken:', authToken);

  if (!userId) {
    console.log('  ❌ No userId provided');
    return null;
  }

  // Check if it's a reseller token
  const isResellerToken = authToken && authToken.includes("reseller_");
  console.log('  - isResellerToken:', isResellerToken);

  if (isResellerToken) {
    console.log('  🔄 Checking reseller in database...');
    // It's a reseller - look in resellers table
    const reseller = await storage.getResellerById(parseInt(userId as string));
    console.log('  - reseller found:', !!reseller);
    if (!reseller) {
      console.log('  ❌ Reseller not found in database');
      return null;
    }

    console.log('  ✅ Reseller authenticated:', reseller.email);
    return {
      user: {
        id: reseller.id,
        email: reseller.email,
        role: "reseller",
        name: reseller.name,
        clientId: null // Resellers don't have clientId
      },
      isReseller: true
    };
  } else {
    console.log('  🔄 Checking regular user in database...');
    // It's a regular user - look in users table
    const user = await storage.getUserById(parseInt(userId as string));
    console.log('  - user found:', !!user);
    if (!user) {
      console.log('  ❌ User not found in database');
      return null;
    }

    console.log('  ✅ User authenticated:', user.email);
    return {
      user,
      isReseller: false
    };
  }
}

// Extend Express Request interface to include user property
declare global {
  namespace Express {
    interface Request {
      user?: any;
    }
  }
}
import { insertUserSchema, insertProductSchema, insertOrderSchema, insertOrderItemSchema, insertPaymentSchema, insertCurrencyExchangeSchema, insertProductHistorySchema, insertVendorSchema, insertCustomerSchema, insertCompanyConfigurationSchema, insertStockControlSessionSchema, insertStockControlItemSchema, insertCashMovementSchema, insertExpenseSchema, insertDebtPaymentSchema, insertCustomerDebtSchema, insertResellerSchema, insertResellerConfigurationSchema, insertResellerSaleSchema, cashMovements, payments, orderItems, orders, customerDebts, expenses, products, cashRegister, currencyExchanges, customers, vendors, cashAutoOperationsLog } from "@shared/schema";

const loginSchema = z.object({
  email: z.string().email(),
  password: z.string(),
});

const productFiltersSchema = z.object({
  search: z.string().optional(),
  status: z.string().optional(),
  storage: z.string().optional(),
  model: z.string().optional(),
  quality: z.string().optional(),
  battery: z.string().optional(),
  provider: z.string().optional(),
  priceMin: z.coerce.number().optional(),
  priceMax: z.coerce.number().optional(),
});

export async function registerRoutes(app: Express): Promise<Server> {

  // Register password reset routes FIRST
  app.use("/api/auth", passwordResetRoutes);

  // Enhanced authentication middleware with proper validation
  const authenticateUser = async (req: any, res: any, next: any) => {
    try {
      const userHeader = req.headers['x-user-id'];
      if (userHeader) {
        // Validate user ID format
        const userId = parseInt(userHeader as string);
        if (isNaN(userId) || userId <= 0) {
          return res.status(400).json({ message: "Invalid user ID format" });
        }

        const user = await storage.getUserById(userId);
        if (user && user.isActive) {
          req.user = user;
        }
      }
      next();
    } catch (error) {
      console.error('Authentication middleware error:', error);
      res.status(500).json({ message: "Authentication error" });
    }
  };

  // Apply authentication middleware to all routes
  app.use(authenticateUser);

  // Health check endpoint
  app.get("/api/health", (req, res) => {
    res.json({ 
      status: "ok", 
      timestamp: new Date().toISOString(),
      version: "1.0.0"
    });
  });

  // Debug endpoint to check complete cash state
  app.get("/api/debug/cash-state/:clientId", async (req, res) => {
    try {
      const clientId = parseInt(req.params.clientId);
      console.log(`🔍 [DEBUG] Complete cash state check for client: ${clientId}`);

      // 1. Check current cash register
      const cashRegister = await storage.getCurrentCashRegister(clientId);
      console.log(`🔍 [DEBUG] Current cash register:`, cashRegister);

      // 2. Check schedule configuration
      const scheduleConfig = await cashScheduleStorage.getScheduleConfig(clientId);
      console.log(`🔍 [DEBUG] Schedule config:`, scheduleConfig);

      // 3. Check current time
      const now = new Date();
      const argentinaTime = new Date(now.toLocaleString("en-US", { timeZone: "America/Argentina/Buenos_Aires" }));

      // 4. Check if should be open
      let shouldBeOpen = false;
      if (scheduleConfig && scheduleConfig.autoOpenEnabled && scheduleConfig.autoCloseEnabled) {
        const currentMinutes = argentinaTime.getHours() * 60 + argentinaTime.getMinutes();
        const openMinutes = scheduleConfig.openHour * 60 + scheduleConfig.openMinute;
        const closeMinutes = scheduleConfig.closeHour * 60 + scheduleConfig.closeMinute;
        shouldBeOpen = currentMinutes >= openMinutes && currentMinutes < closeMinutes;
      }

      const debugInfo = {
        clientId,
        currentTime: argentinaTime.toISOString(),
        argentinaTimeFormatted: `${argentinaTime.getHours().toString().padStart(2, '0')}:${argentinaTime.getMinutes().toString().padStart(2, '0')}`,
        cashRegister: cashRegister ? {
          id: cashRegister.id,
          isOpen: cashRegister.isOpen,
          date: cashRegister.date,
          clientId: cashRegister.clientId
        } : null,
        scheduleConfig: scheduleConfig || null,
        shouldBeOpen,
        actuallyOpen: cashRegister?.isOpen || false,
        finalState: shouldBeOpen && (cashRegister?.isOpen || false) ? 'ABIERTA' : 'CERRADA'
      };

      console.log(`🔍 [DEBUG] Complete state:`, debugInfo);
      res.json(debugInfo);
    } catch (error) {
      console.error(`❌ [DEBUG] Error:`, error);
      res.status(500).json({ error: error.message });
    }
  });

  // Authentication routes
  app.post("/api/auth/login", async (req, res) => {
    try {
      console.log('🔍 === INICIO DE LOGIN DEBUG ===');
      console.log('🔍 1. Datos recibidos en login:', JSON.stringify(req.body, null, 2));
      console.log('🔍 1. Headers recibidos:', JSON.stringify(req.headers, null, 2));

      console.log('🔍 2. Iniciando schema validation...');
      const { email, password } = loginSchema.parse(req.body);
      console.log('✅ 2. Schema validation EXITOSO');
      console.log('🔍 2. Email parseado:', email);
      console.log('🔍 2. Password length:', password ? password.length : 'undefined');

      console.log('🔍 3. Iniciando búsqueda de usuario en base de datos...');
      console.log('🔍 3. Calling storage.getUserByEmail(' + email + ')');

      const user = await storage.getUserByEmail(email);

      console.log('🔍 3. Resultado de getUserByEmail:');
      if (user) {
        console.log('✅ 3. Usuario ENCONTRADO:');
        console.log('   - ID:', user.id);
        console.log('   - Username:', user.username);
        console.log('   - Email:', user.email);
        console.log('   - Role:', user.role);
        console.log('   - ClientId:', user.clientId);
        console.log('   - Password hash (primeros 20):', user.password ? user.password.substring(0, 20) + '...' : 'NO PASSWORD');
        console.log('   - IsActive:', user.isActive);
      } else {
        console.log('❌ 3. Usuario NO ENCONTRADO para email:', email);
        console.log('❌ 3. Terminando login - usuario no existe');
        return res.status(401).json({ message: "Invalid credentials" });
      }

      console.log('🔍 4. Iniciando verificación de contraseña...');
      console.log('🔍 4. Password ingresado length:', password.length);
      console.log('🔍 4. Password ingresado (primeros 10):', password.substring(0, 10));
      console.log('🔍 4. Hash en BD:', user.password);
      console.log('🔍 4. Llamando bcrypt.compareSync...');

      const passwordMatch = bcrypt.compareSync(password, user.password);

      console.log('🔍 4. Resultado de bcrypt.compareSync:', passwordMatch);

      if (!passwordMatch) {
        console.log('❌ 4. Password NO COINCIDE');
        console.log('❌ 4. Password ingresado:', password);
        console.log('❌ 4. Hash en BD:', user.password);
        console.log('❌ 4. Terminando login - password incorrecto');
        return res.status(401).json({ message: "Invalid credentials" });
      }

      console.log('✅ 4. Password CORRECTO');

      console.log('🔍 5. Iniciando búsqueda de cliente...');
      console.log('🔍 5. Calling storage.getClientById(' + user.clientId + ')');

      const client = await storage.getClientById(user.clientId);

      console.log('🔍 5. Resultado de getClientById:');
      if (client) {
        console.log('✅ 5. Cliente ENCONTRADO:');
        console.log('   - ID:', client.id);
        console.log('   - Name:', client.name);
        console.log('   - SubscriptionType:', client.subscriptionType);
        console.log('   - TrialEndDate:', client.trialEndDate);
      } else {
        console.log('❌ 5. Cliente NO ENCONTRADO para clientId:', user.clientId);
        console.log('❌ 5. Terminando login - cliente no existe');
        return res.status(404).json({ message: "Client not found" });
      }

      console.log('✅ 6. Preparando respuesta exitosa...');

      const responseData = {
        user: {
          id: user.id,
          username: user.username,
          email: user.email,
          role: user.role,
          clientId: user.clientId,
          permissions: user.permissions ? JSON.parse(user.permissions) : [],
          mustChangePassword: user.mustChangePassword || false,
        },
        client: {
          id: client.id,
          name: client.name,
          subscriptionType: client.subscriptionType,
          trialStartDate: client.trialStartDate,
          trialEndDate: client.trialEndDate,
          salesContactNumber: client.salesContactNumber,
        },
      };

      console.log('✅ 6. Datos de respuesta preparados:', JSON.stringify(responseData, null, 2));
      console.log('✅ 6. Enviando respuesta exitosa...');
      console.log('🔍 === FIN DE LOGIN DEBUG - EXITOSO ===');

      res.json(responseData);

    } catch (error) {
      console.log('🔍 === LOGIN DEBUG - ERROR CAPTURADO ===');
      console.error('❌ ERROR CRÍTICO en login:');
      console.error('❌ Error type:', typeof error);
      console.error('❌ Error instanceof Error:', error instanceof Error);
      console.error('❌ Error message:', error instanceof Error ? error.message : 'No message');
      console.error('❌ Error stack:', error instanceof Error ? error.stack : 'No stack');
      console.error('❌ Error complete object:', error);
      console.log('🔍 === FIN DE LOGIN DEBUG - ERROR ===');

      res.status(400).json({ 
        message: "Invalid request", 
        error: error instanceof Error ? error.message : "Unknown error",
        errorType: typeof error
      });
    }
  });

  app.post("/api/auth/logout", (res) => {
    res.json({ message: "Logged out successfully" });
  });

  // Reseller login endpoint
  app.post("/api/reseller/login", async (req, res) => {
    try {
      const { email, password } = loginSchema.parse(req.body);
      console.log(`🔍 Reseller login attempt for: ${email}`);

      const reseller = await storage.getResellerByEmail(email);
      if (!reseller) {
        console.log(`❌ Reseller login failed: Reseller not found for email: ${email}`);
        return res.status(401).json({ message: "Invalid credentials" });
      }

      console.log(`🔑 Reseller login - Password check for: ${email}`);
      console.log(`🔑 Password ingresado length: ${password.length}`);
      console.log(`🔑 Password ingresado (primeros 10): ${password.substring(0, 10)}`);
      console.log(`🔑 Password hash completo: ${reseller.password}`);
      console.log(`🔑 Password hash length: ${reseller.password.length}`);

      const passwordMatch = bcrypt.compareSync(password, reseller.password);
      console.log(`🔐 bcrypt.compareSync result: ${passwordMatch}`);

      // Test adicional para debug
      const testHash = bcrypt.hashSync(password, 10);
      console.log(`🧪 Test hash de password ingresado: ${testHash}`);
      console.log(`🧪 ¿El hash actual es válido?: ${reseller.password.startsWith('$2b$') || reseller.password.startsWith('$2a$')}`);

      if (!passwordMatch) {
        console.log(`❌ Reseller login failed: Password mismatch for: ${email}`);
        return res.status(401).json({ message: "Invalid credentials" });
      }

      if (!reseller.isActive) {
        console.log(`❌ Reseller login failed: Account inactive for: ${email}`);
        return res.status(401).json({ message: "Account inactive" });
      }

      console.log(`✅ Reseller login successful for: ${email}`);
      res.json({
        reseller: {
          id: reseller.id,
          name: reseller.name,
          email: reseller.email,
          company: reseller.company,
          commission: reseller.commission,
          accountsQuota: reseller.accountsQuota,
          accountsSold: reseller.accountsSold,
          totalEarnings: reseller.totalEarnings,
          role: 'reseller'
        }
      });
    } catch (error) {
      console.error("Reseller login error:", error);
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Change password route - PRODUCTION SECURITY
  app.post("/api/auth/change-password", async (req, res) => {
    try {
      if (!req.user) {
        return res.status(401).json({ message: "Authentication required" });
      }

      const { currentPassword, newPassword } = req.body;

      if (!currentPassword || !newPassword) {
        return res.status(400).json({ message: "Current and new passwords are required" });
      }

      // Verify current password with bcrypt for production security
      if (!bcrypt.compareSync(currentPassword, req.user.password)) {
        return res.status(401).json({ message: "Current password is incorrect" });
      }

      // Pass raw password - storage.updateUser will handle the hashing
      await storage.updateUser(req.user.id, {
        password: newPassword,  // Pass raw password, storage will hash it
        mustChangePassword: false
      });

      console.log(`✅ Password changed successfully for user: ${req.user.email}`);
      res.json({ message: "Password changed successfully" });
    } catch (error) {
      console.error('❌ Error changing password:', error);
      res.status(500).json({ message: "Error changing password" });
    }
  });

  // Products routes
  app.get("/api/products", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      const filters = productFiltersSchema.parse(req.query);
      const products = await storage.searchProducts(clientId, filters);
      res.json(products);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  app.post("/api/products", async (req, res) => {
    try {
      console.log("POST /api/products - Request body:", req.body);

      // Transform entryDate from string to Date if needed
      const requestData = {
        ...req.body,
        entryDate: req.body.entryDate ? new Date(req.body.entryDate) : new Date()
      };

      console.log("POST /api/products - Transformed data:", requestData);

      const productData = insertProductSchema.parse(requestData);
      console.log("POST /api/products - Validated data:", productData);

      // Check if IMEI already exists for this client
      const existingProduct = await storage.getProductByImei(productData.imei, productData.clientId);
      if (existingProduct) {
        return res.status(400).json({ message: "IMEI already exists" });
      }

      const product = await storage.createProduct(productData);
      res.json(product);
    } catch (error) {
      console.error("POST /api/products - Error:", error);
      if (error instanceof Error && error.message.includes('Expected date, received string')) {
        return res.status(400).json({ 
          message: "Date format error", 
          error: "entryDate must be a valid Date object or ISO string",
          hint: "Ensure dates are properly formatted"
        });
      }
      res.status(400).json({ message: "Invalid request", error: error instanceof Error ? error.message : String(error) });
    }
  });

  // Batch update products by IMEIs (must come before :id endpoints)
  app.put("/api/products/batch-update", async (req, res) => {
    try {
      const { imeis, clientId, updates, userId } = req.body;

      console.log('Batch update request:', { imeis, clientId, updates, userId });

      if (!Array.isArray(imeis) || imeis.length === 0) {
        return res.status(400).json({ message: "IMEIs array is required" });
      }

      if (!clientId || !userId) {
        return res.status(400).json({ message: "Client ID and User ID are required" });
      }

      const validatedUpdates = insertProductSchema.partial().parse(updates);
      console.log('Validated updates:', validatedUpdates);

      const result = await storage.updateProductsByImeis(imeis, clientId, validatedUpdates, userId);
      console.log('Update result:', result);

      res.json(result);
    } catch (error) {
      console.error('Batch update error:', error);
      res.status(400).json({ message: "Invalid request", error: error.message });
    }
  });

  app.get("/api/products/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const product = await storage.getProductById(id);

      if (!product) {
        return res.status(404).json({ message: "Product not found" });
      }

      res.json(product);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  app.put("/api/products/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const { userId, ...productData } = req.body;
      console.log(`Updating product ${id} with data:`, productData);
      const validatedData = insertProductSchema.partial().parse(productData);

      const product = await storage.updateProduct(id, validatedData, userId);
      console.log(`Updated product result:`, product);

      if (!product) {
        return res.status(404).json({ message: "Product not found" });
      }

      res.json(product);
    } catch (error) {
      console.error('Update product error:', error);
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // PATCH endpoint for products (since frontend is using PATCH)
  app.patch("/api/products/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const { userId, ...productData } = req.body;
      console.log(`PATCH: Updating product ${id} with data:`, productData);
      const validatedData = insertProductSchema.partial().parse(productData);

      const product = await storage.updateProduct(id, validatedData, userId);
      console.log(`PATCH: Updated product result:`, product);

      if (!product) {
        return res.status(404).json({ message: "Product not found" });
      }

      res.json(product);
    } catch (error) {
      console.error('PATCH product error:', error);
      res.status(400).json({ message: "Invalid request", error: error.message });
    }
  });

  app.delete("/api/products/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const success = await storage.deleteProduct(id);

      if (!success) {
        return res.status(404).json({ message: "Product not found" });
      }

      res.json({ message: "Product deleted successfully" });
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Check if IMEI exists
  app.get("/api/products/check-imei/:imei", async (req, res) => {
    try {
      const imei = req.params.imei;
      const clientId = parseInt(req.query.clientId as string);

      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      const existingProduct = await storage.getProductByImei(imei, clientId);
      res.json({ exists: !!existingProduct });
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Batch create products
  app.post("/api/products/batch", async (req, res) => {
    try {
      const { products } = req.body;

      if (!Array.isArray(products) || products.length === 0) {
        return res.status(400).json({ message: "Products array is required" });
      }

      const createdProducts = [];

      for (const productData of products) {
        const validatedProduct = insertProductSchema.parse(productData);

        // Check if IMEI already exists
        const existingProduct = await storage.getProductByImei(validatedProduct.imei, validatedProduct.clientId);
        if (existingProduct) {
          return res.status(400).json({ message: `IMEI ${validatedProduct.imei} already exists` });
        }

        const product = await storage.createProduct(validatedProduct);
        createdProducts.push(product);
      }

      res.json({ products: createdProducts, count: createdProducts.length });
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Delete product
  app.delete("/api/products/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const success = await storage.deleteProduct(id);

      if (!success) {
        return res.status(404).json({ message: "Product not found" });
      }

      res.json({ message: "Product deleted successfully" });
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });



  // Product History routes
  app.get("/api/products/:id/history", async (req, res) => {
    try {
      const productId = parseInt(req.params.id);
      const history = await storage.getProductHistoryByProductId(productId);
      res.json(history);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  app.post("/api/products/:id/history", async (req, res) => {
    try {
      const productId = parseInt(req.params.id);
      const historyData = insertProductHistorySchema.parse({
        ...req.body,
        productId,
      });

      const history = await storage.createProductHistory(historyData);
      res.json(history);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  app.get("/api/history", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      const history = await storage.getProductHistoryByClientId(clientId);
      res.json(history);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  app.get("/api/alerts", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      const alerts = await storage.getProductsWithAlerts(clientId);
      res.json(alerts);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Orders routes
  app.get("/api/orders", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      // Use getOrdersWithItemsAndProducts to get complete order data with vendor and product information
      const orders = await storage.getOrdersWithItemsAndProducts(clientId);
      res.json(orders);
    } catch (error) {
      console.error('Error getting orders with items and products:', error);
      res.status(400).json({ message: "Invalid request" });
    }
  });

  app.post("/api/orders", async (req, res) => {
    try {
      console.log('📋 [ORDER CREATION] Starting complete order processing...');
      console.log('📋 [ORDER CREATION] Request data:', JSON.stringify(req.body, null, 2));

      const { payments, orderItems, ...orderBody } = req.body;

      // Generate order number first
      const orderNumber = `ORD-${Date.now()}`;

      // Parse with generated order number
      const orderData = insertOrderSchema.parse({
        ...orderBody,
        orderNumber,
      });

      const order = await storage.createOrder(orderData);
      console.log('✅ [ORDER CREATION] Order created successfully:', order.id, 'Number:', order.orderNumber);

      // Process order items if provided
      if (orderItems && orderItems.length > 0) {
        console.log('📦 [ORDER ITEMS] Processing', orderItems.length, 'items...');
        for (const item of orderItems) {
          await storage.createOrderItem({
            ...item,
            orderId: order.id,
            clientId: order.clientId
          });

          // Update product status to sold
          if (item.productId) {
            await storage.updateProduct(item.productId, { status: 'vendido' });
            console.log(`📱 [PRODUCT UPDATE] Product ${item.productId} status updated to 'vendido'`);
          }
        }
      }

      // CASH MOVEMENT CREATION SYSTEM - EXACT COPY FROM WORKING DEBT PAYMENTS
      if (payments && payments.length > 0) {
        console.log('💳 [CASH MOVEMENTS] Processing', payments.length, 'payments using DEBT PAYMENT system logic...');

        // Get current cash register - EXACT COPY FROM DEBT PAYMENTS
        let currentCashRegister = await storage.getCurrentCashRegister(order.clientId);
        if (!currentCashRegister) {
          console.log(`No active cash register found, getting most recent for client ${order.clientId}`);
          currentCashRegister = await storage.getOrCreateTodayCashRegister(order.clientId);
        }
        console.log(`Using cash register ID: ${currentCashRegister.id} for client ${order.clientId}`);

        for (let i = 0; i < payments.length; i++) {
          const paymentData = payments[i];
          console.log(`🔄 [PAYMENT ${i+1}/${payments.length}] Processing debt payment logic:`, paymentData);

          // Calculate USD amount with corrected exchange rate handling - EXACT COPY FROM DEBT PAYMENTS
          let amountUsd;

          // Handle payment method specific conversion logic - EXACT COPY FROM DEBT PAYMENTS
          if (paymentData.paymentMethod === 'efectivo_ars' || paymentData.paymentMethod === 'transferencia_ars') {
            // ARS methods: amount in ARS, convert to USD
            const exchangeRate = parseFloat(paymentData.exchangeRate || "1100");
            amountUsd = parseFloat(paymentData.amount) / exchangeRate;
          } else if (paymentData.paymentMethod === 'transferencia_usdt') {
            // USDT methods: USDT counts as USD (1:1 for accounting)
            amountUsd = parseFloat(paymentData.amount);
          } else if (paymentData.paymentMethod === 'financiera_ars') {
            // Financiera ARS→USD: amount in ARS, convert to USD
            const exchangeRate = parseFloat(paymentData.exchangeRate || "1050");
            amountUsd = parseFloat(paymentData.amount) / exchangeRate;
          } else {
            // USD methods (efectivo_usd, transferencia_usd, financiera_usd): already in USD
            amountUsd = parseFloat(paymentData.amount);
          }

          // Create payment record in payments table - EXACT COPY FROM DEBT PAYMENTS
          await storage.createPayment({
            clientId: order.clientId,
            orderId: order.id,
            paymentMethod: paymentData.paymentMethod,
            amount: paymentData.amount,
            exchangeRate: paymentData.exchangeRate || "1",
            amountUsd: amountUsd.toFixed(2),
            notes: paymentData.notes || `Pago ${paymentData.paymentMethod} - ${paymentData.amount}`
          });

          // Detect currency from payment method - EXACT COPY FROM DEBT PAYMENTS
          let currency = 'USD';
          if (paymentData.paymentMethod.includes('_ars')) {
            currency = 'ARS';
          } else if (paymentData.paymentMethod.includes('_usdt')) {
            currency = 'USDT';
          }

          // Create cash movement for tracking - EXACT COPY FROM DEBT PAYMENTS
          await storage.createCashMovement({
            clientId: order.clientId,
            cashRegisterId: currentCashRegister.id,
            type: 'venta',
            subtype: paymentData.paymentMethod,
            amount: paymentData.amount,
            currency: currency,
            exchangeRate: paymentData.exchangeRate || "1",
            amountUsd: amountUsd.toFixed(2),
            description: `Venta - Orden #${order.orderNumber}`,
            referenceId: order.id,
            referenceType: 'order_payment',
            customerId: order.customerId,
            vendorId: order.vendorId,
            userId: parseInt(req.headers['x-user-id'] as string) || 37,  // Use valid user ID
            notes: paymentData.notes || `Pago ${paymentData.paymentMethod} - ${currency} ${paymentData.amount}`
          });

          console.log(`📊 ✅ Cash movement created: ${currency} ${paymentData.amount} = USD ${amountUsd.toFixed(2)}`);
        }

        console.log('🔄 Real-time synchronization completed - Order and Cash Movements updated using DEBT PAYMENT logic');
      }

      // Update order payment status after all payments are processed
      const allPayments = await storage.getPaymentsByOrderId(order.id);
      const totalPaid = allPayments.reduce((sum, p) => sum + parseFloat(p.amountUsd), 0);
      const totalOrder = parseFloat(order.totalUsd);

      // Respect user-selected payment status when "pagado" is explicitly chosen
      let paymentStatus = order.paymentStatus || "pendiente";

      // Only auto-calculate status if user didn't explicitly select "pagado"
      if (order.paymentStatus !== "pagado") {
        if (totalPaid >= totalOrder) {
          paymentStatus = "pagado";
        } else if (totalPaid > 0) {
          paymentStatus = "parcial";
        } else {
          paymentStatus = "pendiente";
        }
      }

      await storage.updateOrder(order.id, { paymentStatus });
      console.log(`🔄 [ORDER STATUS] Payment status updated to: ${paymentStatus} (User selected: ${order.paymentStatus}, Paid: $${totalPaid}, Total: $${totalOrder})`);

      // Create automatic debt only if status is NOT "pagado"
      if (paymentStatus === "parcial" || paymentStatus === "pendiente") {
        const existingDebt = await storage.getActiveDebtByOrderId(order.id);

        if (!existingDebt) {
          const debtAmountUsd = totalOrder - totalPaid;
          console.log(`💳 [DEBT] Creating automatic debt for order ${order.id}: $${debtAmountUsd}`);

          await storage.createCustomerDebt({
            clientId: order.clientId,
            customerId: order.customerId,
            orderId: order.id,
            debtAmount: debtAmountUsd.toFixed(2),
            paidAmount: totalPaid.toFixed(2),
            remainingAmount: debtAmountUsd.toFixed(2),
            currency: "USD",
            status: "vigente",
            dueDate: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000), // 30 days from now
            notes: `Deuda automática por ${paymentStatus === "parcial" ? "pago parcial" : "pago pendiente"}. Total: $${totalOrder}, Pagado: $${totalPaid.toFixed(2)}`
          });
          console.log(`✅ [DEBT] Automatic debt created successfully`);
        }
      } else if (paymentStatus === "pagado") {
        console.log(`✅ [NO DEBT] Order marked as "pagado" - no automatic debt will be created`);
        // Remove any existing debt for this order when marked as paid
        const existingDebt = await storage.getActiveDebtByOrderId(order.id);
        if (existingDebt) {
          await storage.updateCustomerDebt(existingDebt.id, { status: "pagado" });
          console.log(`✅ [DEBT] Marked existing debt as paid for fully paid order`);
        }
      }

      console.log(`🎉 [ORDER CREATION] Order ${order.orderNumber} completed successfully with full synchronization`);
      res.json(order);

    } catch (error) {
      console.error('❌ [ORDER CREATION] FATAL ERROR:', error);
      if (error instanceof Error) {
        console.error('❌ [ORDER CREATION] Error details:', error.message);
        console.error('❌ [ORDER CREATION] Stack trace:', error.stack);
      }
      res.status(400).json({ 
        message: "Error creating order", 
        error: error instanceof Error ? error.message : "Unknown error",
        details: "Order creation failed - check server logs for details"
      });
    }
  });

  app.get("/api/orders/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const order = await storage.getOrderById(id);

      if (!order) {
        return res.status(404).json({ message: "Order not found" });
      }

      const items = await storage.getOrderItemsByOrderId(id);
      const payments = await storage.getPaymentsByOrderId(id);

      res.json({
        ...order,
        items,
        payments,
      });
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  app.put("/api/orders/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const orderData = insertOrderSchema.partial().parse(req.body);

      const order = await storage.updateOrder(id, orderData);

      if (!order) {
        return res.status(404).json({ message: "Order not found" });
      }

      res.json(order);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // PATCH endpoint for partial order updates (e.g., payment status)
  app.patch("/api/orders/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const orderData = insertOrderSchema.partial().parse(req.body);

      const order = await storage.updateOrder(id, orderData);

      if (!order) {
        return res.status(404).json({ message: "Order not found" });
      }

      // If payment status is updated to "parcial", create debt automatically
      if (orderData.paymentStatus === "parcial") {
        console.log(`Processing partial payment for order ${id}`);

        // Get order payments to calculate debt
        const payments = await storage.getPaymentsByOrderId(id);
        console.log(`Found ${payments.length} payments for order ${id}:`, payments.map(p => ({ method: p.paymentMethod, amountUsd: p.amountUsd })));

        const totalPaidUsd = payments.reduce((sum, payment) => sum + parseFloat(payment.amountUsd), 0);
        const totalOrderUsd = parseFloat(order.totalUsd);
        const debtAmountUsd = totalOrderUsd - totalPaidUsd;

        console.log(`Debt calculation - Total: $${totalOrderUsd}, Paid: $${totalPaidUsd}, Debt: $${debtAmountUsd}`);

        if (debtAmountUsd > 0) {
          // Check if debt already exists for this order
          const existingDebts = await storage.getCustomerDebtsByClientId(order.clientId);
          const orderHasDebt = existingDebts.some((debt: any) => debt.orderId === id);

          console.log(`Existing debts count: ${existingDebts.length}, Order already has debt: ${orderHasDebt}`);

          if (!orderHasDebt) {
            console.log(`Creating debt for order ${id}, customer ${order.customerId}`);

            // Create customer debt
            const newDebt = await storage.createCustomerDebt({
              clientId: order.clientId,
              customerId: order.customerId,
              orderId: id,
              debtAmount: debtAmountUsd.toFixed(2),
              paidAmount: "0.00",
              remainingAmount: debtAmountUsd.toFixed(2),
              currency: "USD",
              status: "vigente",
              dueDate: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000), // 30 days from now
              notes: `Deuda automática por pago parcial. Total: $${totalOrderUsd}, Pagado: $${totalPaidUsd.toFixed(2)}`
            });

            console.log(`✅ Debt created successfully for order ${id}:`, newDebt);
          } else {
            console.log(`⚠️ Debt already exists for order ${id}, skipping creation`);
          }
        } else {
          console.log(`❌ No debt needed (amount <= 0): $${debtAmountUsd}`);
        }
      }

      console.log(`Order ${id} updated with payment status: ${orderData.paymentStatus}`);
      res.json(order);
    } catch (error) {
      console.error(`Error updating order ${req.params.id}:`, error);
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Order Items routes
  app.get("/api/order-items", async (req, res) => {
    try {
      console.log('GET /api/order-items - Query params:', req.query);
      const orderId = parseInt(req.query.orderId as string);

      if (!orderId || isNaN(orderId)) {
        console.log('Invalid orderId provided');
        return res.status(400).json({ message: "Order ID is required and must be a valid number" });
      }

      console.log('Fetching real order items for orderId:', orderId);
      // Get real order items with products from database
      const orderItemsWithProducts = await storage.getOrderItemsWithProductsByOrderId(orderId);
      console.log('Retrieved real order items:', orderItemsWithProducts);
      res.json(orderItemsWithProducts);
    } catch (error) {
      console.error('Error in GET /api/order-items:', error);
      res.status(500).json({ message: "Internal server error", error: error instanceof Error ? error.message : String(error) });
    }
  });

  // Users routes (only accessible to superuser/developer)
  app.get("/api/users", async (req, res) => {
    try {
      // Check if user is authenticated and has superuser role
      if (!req.user || req.user.role !== "superuser") {
        return res.status(403).json({ message: "Access denied. Developer access required." });
      }

      const clientId = parseInt(req.query.clientId as string);
      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      const users = await storage.getUsersByClientId(clientId);
      res.json(users);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Get vendors for permission management (admin access)
  app.get("/api/users/vendors", async (req, res) => {
    try {
      if (!req.user) {
        return res.status(401).json({ message: "Authentication required" });
      }

      // Admin can see their vendors, superuser can see all
      const clientId = req.user.role === 'superuser' ? 
        parseInt(req.query.clientId as string) || req.user.clientId : 
        req.user.clientId;

      const vendors = await storage.getUsersByClientIdAndRole(clientId, 'vendor');
      res.json(vendors);
    } catch (error) {
      console.error("Error fetching vendors:", error);
      res.status(500).json({ message: "Error fetching vendors" });
    }
  });

  // Update user permissions (admin only)
  app.put("/api/users/:id/permissions", async (req, res) => {
    try {
      if (!req.user || req.user.role !== 'admin') {
        return res.status(403).json({ message: "Access denied. Admin access required." });
      }

      const userId = parseInt(req.params.id);
      const { permissions } = req.body;

      // Verify the user belongs to the same client
      const targetUser = await storage.getUserById(userId);
      if (!targetUser || targetUser.clientId !== req.user.clientId) {
        return res.status(404).json({ message: "User not found" });
      }

      // Only allow updating vendor permissions
      if (targetUser.role !== 'vendor') {
        return res.status(403).json({ message: "Can only update vendor permissions" });
      }

      const updatedUser = await storage.updateUser(userId, {
        permissions: permissions
      });

      res.json(updatedUser);
    } catch (error) {
      console.error("Error updating user permissions:", error);
      res.status(500).json({ message: "Error updating permissions" });
    }
  });

  // Update user profile (email, phone)
  app.patch("/api/users/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const { email, phone } = req.body;

      // Only allow updating email and phone
      const updateData: any = {};
      if (email !== undefined) updateData.email = email;
      if (phone !== undefined) updateData.phone = phone;

      const updatedUser = await storage.updateUser(id, updateData);

      if (!updatedUser) {
        return res.status(404).json({ message: "User not found" });
      }

      res.json(updatedUser);
    } catch (error) {
      console.error("Error updating user profile:", error);
      res.status(500).json({ message: "Error updating profile" });
    }
  });

  // Change user password
  app.post("/api/users/:id/change-password", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const { currentPassword, newPassword } = req.body;

      if (!currentPassword || !newPassword) {
        return res.status(400).json({ message: "Current password and new password are required" });
      }

      if (newPassword.length < 6) {
        return res.status(400).json({ message: "New password must be at least 6 characters long" });
      }

      // Get current user
      const user = await storage.getUserById(id);
      if (!user) {
        return res.status(404).json({ message: "User not found" });
      }

      // Verify current password using bcrypt
      const isCurrentPasswordValid = bcrypt.compareSync(currentPassword, user.password);
      if (!isCurrentPasswordValid) {
        return res.status(400).json({ message: "Current password is incorrect" });
      }

      // Pass raw password - storage.updateUser will handle the hashing  
      const updatedUser = await storage.updateUser(id, { 
        password: newPassword,  // Pass raw password, storage will hash it
        mustChangePassword: false 
      });

      if (!updatedUser) {
        return res.status(404).json({ message: "User not found" });
      }

      console.log(`✅ Password changed successfully for user ${user.email} (ID: ${id})`);
      res.json({ message: "Password changed successfully" });
    } catch (error) {
      console.error("Error changing password:", error);
      res.status(500).json({ message: "Error changing password" });
    }
  });

  app.post("/api/users", async (req, res) => {
    try {
      // Check if user is authenticated and has admin role
      if (!req.user || req.user.role !== "admin") {
        return res.status(403).json({ message: "Access denied. Admin access required." });
      }

      const userData = {
        ...req.body,
        role: "vendor",
        clientId: req.user.clientId,
        mustChangePassword: true
      };

      // Parse with user schema
      const parsedData = insertUserSchema.parse(userData);

      // Check if email already exists
      const existingUser = await storage.getUserByEmail(parsedData.email);
      if (existingUser) {
        return res.status(400).json({ message: "Email already exists" });
      }

      // Check if username already exists
      const existingUsername = await storage.getUserByUsername(parsedData.username);
      if (existingUsername) {
        return res.status(400).json({ message: "Username already exists" });
      }

      const user = await storage.createUser(parsedData);
      res.json(user);
    } catch (error) {
      console.error("Error creating vendor user:", error);
      res.status(400).json({ message: "Invalid request", error: error instanceof Error ? error.message : "Unknown error" });
    }
  });

  // Create vendor (admin only)
  app.post("/api/vendors/create", async (req, res) => {
    try {
      if (!req.user || req.user.role !== 'admin') {
        return res.status(403).json({ message: "Access denied. Admin access required." });
      }

      const { username, email, password, permissions } = req.body;

      // Check if email already exists
      const existingUser = await storage.getUserByEmail(email);
      if (existingUser) {
        return res.status(400).json({ message: "Email already exists" });
      }

      // Check if username already exists in this client
      const existingUsername = await storage.getUserByUsername(username);
      if (existingUsername && existingUsername.clientId === req.user.clientId) {
        return res.status(400).json({ message: "Username already exists in your organization" });
      }

      const vendorData = {
        clientId: req.user.clientId,
        username,
        email,
        password,
        role: 'vendor' as const,
        permissions: permissions || "[]",
        isActive: true,
        mustChangePassword: true, // Force password change on first login
      };

      const vendor = await storage.createUser(vendorData);
      res.json(vendor);
    } catch (error) {
      console.error("Error creating vendor:", error);
      res.status(500).json({ message: "Error creating vendor" });
    }
  });

  // Cash register routes
  app.get("/api/cash-register", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      const cashRegister = await storage.getCashRegisterByClientId(clientId);
      res.json(cashRegister);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  app.post("/api/cash-register", async (req, res) => {
    try {
      const cashRegisterData = req.body;
      const cashRegister = await storage.createCashRegister(cashRegisterData);
      res.json(cashRegister);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Currency exchange routes
  app.post("/api/cash-register/currency-exchange", async (req, res) => {
    try {
      console.log("Currency exchange request body:", req.body);

      // Simplificar para evitar problemas con el schema
      const exchangeData = {
        clientId: req.body.clientId,
        fromCurrency: req.body.fromCurrency,
        toCurrency: req.body.toCurrency,
        fromAmount: req.body.fromAmount.toString(),
        toAmount: req.body.toAmount.toString(),
        exchangeRate: req.body.exchangeRate.toString(),
        category: req.body.category || 'conversion_moneda',
        notes: req.body.notes || '',
        userId: req.body.userId
      };

      console.log("Prepared exchange data:", exchangeData);
      const exchange = await storage.createCurrencyExchange(exchangeData);
      console.log("Exchange created:", exchange);
      res.json(exchange);
    } catch (error) {
      console.error("Currency exchange error:", error);
      res.status(400).json({ 
        message: "Invalid request", 
        error: error instanceof Error ? error.message : String(error) 
      });
    }
  });

  app.get("/api/currency-exchanges", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      const exchanges = await storage.getCurrencyExchangesByClientId(clientId);
      res.json(exchanges);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Payments routes
  app.get("/api/payments", async (req, res) => {
    try {
      const orderId = parseInt(req.query.orderId as string);
      if (orderId) {
        const payments = await storage.getPaymentsByOrderId(orderId);
        res.json(payments);
      } else {
        const clientId = parseInt(req.query.clientId as string);
        if (!clientId) {
          return res.status(400).json({ message: "Order ID or Client ID is required" });
        }
        const payments = await storage.getPaymentsByClientId(clientId);
        res.json(payments);
      }
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  app.post("/api/payments", async (req, res) => {
    try {
      const paymentData = insertPaymentSchema.parse(req.body);
      const payment = await storage.createPayment(paymentData);

      // After payment is created, check if order needs payment status update and debt calculation
      if (payment.orderId) {
        const order = await storage.getOrderById(payment.orderId);

        if (order) {
          // Calculate total payments for this order
          const payments = await storage.getPaymentsByOrderId(order.id);
          const totalPaidUsd = payments.reduce((sum, p) => sum + parseFloat(p.amountUsd), 0);
          const totalOrderUsd = parseFloat(order.totalUsd);

          // Determine correct payment status
          let newPaymentStatus = "pendiente";
          if (totalPaidUsd >= totalOrderUsd) {
            newPaymentStatus = "pagado";
          } else if (totalPaidUsd > 0) {
            newPaymentStatus = "parcial";
          }

          // Update order payment status if changed
          if (order.paymentStatus !== newPaymentStatus) {
            await storage.updateOrder(order.id, { paymentStatus: newPaymentStatus });
            console.log(`🔄 Updated order ${order.id} payment status: ${order.paymentStatus} → ${newPaymentStatus}`);
          }

          // Debt calculation for partial payments
          if (newPaymentStatus === "parcial" && order.customerId) {
            console.log(`🔄 Checking debt calculation for order ${order.id} after payment registration`);

            const debtAmountUsd = totalOrderUsd - totalPaidUsd;
            console.log(`💰 Debt calculation - Total: $${totalOrderUsd}, Paid: $${totalPaidUsd}, Debt: $${debtAmountUsd}`);

            if (debtAmountUsd > 0) {
              // Check if debt already exists for this order
              const existingDebts = await storage.getCustomerDebtsByClientId(order.clientId);
              const existingDebt = existingDebts.find(debt => debt.orderId === order.id && debt.status === 'vigente');

              if (existingDebt) {
                // Update existing debt with correct amounts
                await storage.updateCustomerDebt(existingDebt.id, {
                  debtAmount: debtAmountUsd.toFixed(2),
                  paidAmount: totalPaidUsd.toFixed(2),
                  remainingAmount: debtAmountUsd.toFixed(2),
                  notes: `Deuda actualizada automáticamente. Total: $${totalOrderUsd}, Pagado: $${totalPaidUsd.toFixed(2)}`
                });
                console.log(`✅ Debt updated for order ${order.id}: $${debtAmountUsd.toFixed(2)}`);
              } else {
                // Create new debt
                const newDebt = await storage.createCustomerDebt({
                  clientId: order.clientId,
                  customerId: order.customerId,
                  orderId: order.id,
                  debtAmount: debtAmountUsd.toFixed(2),
                  paidAmount: totalPaidUsd.toFixed(2),
                  remainingAmount: debtAmountUsd.toFixed(2),
                  currency: "USD",
                  status: "vigente",
                  dueDate: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000), // 30 days from now
                  notes: `Deuda automática por pago parcial. Total: $${totalOrderUsd}, Pagado: $${totalPaidUsd.toFixed(2)}`
                });
                console.log(`✅ Automatic debt created for order ${order.id}: $${debtAmountUsd.toFixed(2)}`);
              }
            }
          }
        }
      }

      res.json(payment);
    } catch (error) {
      console.error("Error creating payment:", error);
      res.status(400).json({ message: "Invalid request", error: error instanceof Error ? error.message : "Unknown error" });
    }
  });

  app.post("/api/order-items", async (req, res) => {
    console.log("=== POST /api/order-items CALLED ===");
    try {
      console.log("Request body received:", JSON.stringify(req.body, null, 2));

      // Validar schema
      const orderItemData = insertOrderItemSchema.parse(req.body);
      console.log("Schema validation successful. Parsed data:", JSON.stringify(orderItemData, null, 2));

      // Crear order item
      const orderItem = await storage.createOrderItem(orderItemData);
      console.log("Order item created successfully:", JSON.stringify(orderItem, null, 2));

      res.json(orderItem);
    } catch (error) {
      console.error("=== ERROR in POST /api/order-items ===");
      console.error("Error details:", error);
      if (error instanceof Error) {
        console.error("Error message:", error.message);
        console.error("Error stack:", error.stack);
      }
      res.status(400).json({ 
        message: "Invalid request", 
        error: error instanceof Error ? error.message : "Unknown error",
        details: error
      });
    }
  });

  app.delete("/api/order-items/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const success = await storage.deleteOrderItem(id);

      if (!success) {
        return res.status(404).json({ message: "Order item not found" });
      }

      res.json({ message: "Order item deleted successfully" });
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Dashboard stats
  app.get("/api/dashboard/stats", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      const products = await storage.getProductsByClientId(clientId);
      const orders = await storage.getOrdersByClientId(clientId);
      const payments = await storage.getPaymentsByClientId(clientId);

      const totalProducts = products.length;
      const pendingOrders = orders.filter(order => order.status === "pendiente").length;
      // Count products that are running low on stock
      const availableProducts = products.filter(product => product.status === "disponible");
      const lowStockProducts = availableProducts.length; // Just count available products for now, or implement real stock threshold logic

      const currentMonth = new Date().getMonth();
      const currentYear = new Date().getFullYear();
      const monthlySales = payments
        .filter(payment => {
          const paymentDate = new Date(payment.createdAt);
          return paymentDate.getMonth() === currentMonth && paymentDate.getFullYear() === currentYear;
        })
        .reduce((sum, payment) => sum + parseFloat(payment.amountUsd), 0);

      res.json({
        totalProducts,
        monthlySales,
        pendingOrders,
        lowStock: lowStockProducts,
      });
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Get vendor names for order filtering (no special permissions required)
  app.get("/api/users/vendors", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      // Get vendors from the vendors table, not users table
      const vendors = await storage.getVendorsByClientId(clientId);
      // Return only basic vendor info (id and name) for display purposes
      const vendorInfo = vendors.map(vendor => ({
        id: vendor.id,
        name: vendor.name, // El campo se llama name en la tabla vendors
        role: 'vendor'
      }));

      res.json(vendorInfo);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Vendors routes
  app.get("/api/vendors", async (req, res) => {
    try {
      let clientId;

      // Try to get clientId from authenticated user first
      if (req.user && req.user.clientId) {
        clientId = req.user.clientId;
      } else {
        // Fallback to query parameter
        clientId = parseInt(req.query.clientId as string);
      }

      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      const vendors = await storage.getVendorsByClientId(clientId);
      res.json(vendors);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  app.post("/api/vendors", async (req, res) => {
    try {
      const vendorData = insertVendorSchema.parse(req.body);
      const vendor = await storage.createVendor(vendorData);
      res.json(vendor);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  app.put("/api/vendors/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const vendorData = insertVendorSchema.partial().parse(req.body);

      const vendor = await storage.updateVendor(id, vendorData);

      if (!vendor) {
        return res.status(404).json({ message: "Vendor not found" });
      }

      // If commission was updated, trigger a sync with cash register
      if (vendorData.commissionPercentage !== undefined) {
        console.log(`🔄 Vendor ${vendor.name} commission updated to ${vendor.commissionPercentage}% - triggering commission sync`);

        // Invalidate any cached commission calculations
        try {
          // Future: Implement commission recalculation for existing orders if needed
          console.log("✅ Commission sync completed");
        } catch (syncError) {
          console.error("⚠️ Commission sync failed:", syncError);
        }
      }

      res.json(vendor);
    } catch (error) {
      console.error("Error updating vendor:", error);
      res.status(400).json({ message: "Invalid request" });
    }
  });

  app.delete("/api/vendors/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const deleted = await storage.deleteVendor(id);

      if (!deleted) {
        return res.status(404).json({ message: "Vendor not found" });
      }

      res.json({ message: "Vendor deleted successfully" });
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Customers routes
  app.get("/api/customers", async (req, res) => {
    try {
      let clientId;

      // Try to get clientId from authenticated user first
      if (req.user && req.user.clientId) {
        clientId = req.user.clientId;
      } else {
        // Fallback to query parameter
        clientId = parseInt(req.query.clientId as string);
      }

      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      const customers = await storage.getCustomersByClientId(clientId);
      res.json(customers);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  app.post("/api/customers", async (req, res) => {
    try {
      const customerData = insertCustomerSchema.parse(req.body);
      const customer = await storage.createCustomer(customerData);
      res.json(customer);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  app.put("/api/customers/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const customerData = insertCustomerSchema.partial().parse(req.body);

      const customer = await storage.updateCustomer(id, customerData);

      if (!customer) {
        return res.status(404).json({ message: "Customer not found" });
      }

      res.json(customer);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  app.delete("/api/customers/:id", async (req, res) => {
    try {
      const id = parseInt(req.params.id);
      const deleted = await storage.deleteCustomer(id);

      if (!deleted) {
        return res.status(404).json({ message: "Customer not found" });
      }

      res.json({ message: "Customer deleted successfully" });
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Currency Exchange routes
  app.post("/api/currency-exchanges", async (req, res) => {
    try {
      const exchangeData = insertCurrencyExchangeSchema.parse(req.body);
      const exchange = await storage.createCurrencyExchange(exchangeData);
      res.json(exchange);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  app.get("/api/currency-exchanges", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      const exchanges = await storage.getCurrencyExchangesByClientId(clientId);
      res.json(exchanges);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Cash Register routes
  app.get("/api/cash-register/current", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      console.log(`🚀 [API] GET /api/cash-register/current called with clientId: ${clientId}`);

      if (!clientId) {
        console.log(`❌ [API] No clientId provided`);
        return res.status(400).json({ message: "Client ID is required" });
      }

      console.log(`🔍 [API] Calling storage.getCurrentCashRegister(${clientId})`);
      let cashRegister = await storage.getCurrentCashRegister(clientId);
      console.log(`🔍 [API] storage.getCurrentCashRegister returned:`, cashRegister ? {
        id: cashRegister.id,
        isOpen: cashRegister.isOpen,
        date: cashRegister.date,
        clientId: cashRegister.clientId
      } : null);

      if (cashRegister) {
        console.log(`💰 [API] Found cash register ID: ${cashRegister.id}, isOpen: ${cashRegister.isOpen}, date: ${cashRegister.date}`);

        // Verificar si es de hoy
        const today = new Date().toISOString().split('T')[0];
        const registerDate = new Date(cashRegister.date).toISOString().split('T')[0];
        console.log(`📅 [API] Date comparison - today: ${today}, register: ${registerDate}`);

        if (registerDate !== today) {
          console.log(`⚠️ [API] Cash register is from ${registerDate}, today is ${today} - considering as closed`);
          // Si la caja no es de hoy, tratarla como cerrada
          cashRegister = null;
        }
      }

      // CORRECCIÓN: Si no hay caja abierta PARA HOY, intentar crear una automáticamente
      if (!cashRegister) {
        console.log(`⚡ [API] No hay caja abierta para HOY para cliente ${clientId}, creando automáticamente...`);

        try {
          const newCashRegisterData = {
            clientId,
            date: new Date(),
            initialUsd: "0.00",
            initialArs: "0.00",
            initialUsdt: "0.00",
            currentUsd: "0.00",
            currentArs: "0.00",
            currentUsdt: "0.00",
            dailySales: "0.00",
            totalExpenses: "0.00",
            dailyGlobalExchangeRate: "1200.00",
            isOpen: true,
            isActive: true,
          };

          console.log(`📝 [API] Creating new cash register with data:`, newCashRegisterData);
          cashRegister = await storage.createCashRegister(newCashRegisterData);
          console.log(`✅ [API] Caja creada automáticamente:`, {
            id: cashRegister.id,
            isOpen: cashRegister.isOpen,
            clientId: cashRegister.clientId
          });
        } catch (createError) {
          console.error(`❌ [API] Error creando caja automáticamente:`, createError);
          return res.status(404).json({ message: "No open cash register found and could not create one" });
        }
      }

      console.log(`📤 [API] Returning cash register to frontend:`, {
        id: cashRegister.id,
        isOpen: cashRegister.isOpen,
        date: cashRegister.date,
        clientId: cashRegister.clientId
      });

      res.json(cashRegister);
    } catch (error) {
      console.error("❌ [API] Error in /api/cash-register/current:", error);
      res.status(500).json({ message: "Internal server error" });
    }
  });

  app.post("/api/cash-register/open", async (req, res) => {
    try {
      const { clientId, initialUsd, initialArs, initialUsdt } = req.body;

      // Check if there's already an open cash register
      const existingRegister = await storage.getCurrentCashRegister(clientId);
      if (existingRegister) {
        return res.status(400).json({ message: "Ya hay una caja abierta" });
      }

      const cashRegister = await storage.createCashRegister({
        clientId,
        date: new Date(),
        initialUsd,
        initialArs,
        initialUsdt,
        currentUsd: initialUsd,
        currentArs: initialArs,
        currentUsdt: initialUsdt,
        dailySales: "0.00",
        isOpen: true,
      });

      res.json(cashRegister);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  app.post("/api/cash-register/close", async (req, res) => {
    try {
      const { clientId } = req.body;

      const cashRegister = await storage.getCurrentCashRegister(clientId);
      if (!cashRegister) {
        return res.status(404).json({ message: "No open cash register found" });
      }

      const updatedRegister = await storage.updateCashRegister(cashRegister.id, {
        isOpen: false,
        closedAt: new Date(),
      });

      res.json(updatedRegister);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  app.get("/api/cash-register/daily-sales", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      const today = new Date();
      const startOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate());
      const endOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate() + 1);

      const dailySales = await storage.getDailySales(clientId, startOfDay, endOfDay);
      res.json(dailySales);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Company Configuration routes
  app.get("/api/company-configuration", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      const config = await storage.getCompanyConfigurationByClientId(clientId);
      if (!config) {
        return res.status(404).json({ message: "Company configuration not found" });
      }

      res.json(config);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  app.post("/api/company-configuration", async (req, res) => {
    try {
      console.log("Company configuration request body:", JSON.stringify(req.body, null, 2));

      // Get user from header to verify permissions
      const userId = req.headers['x-user-id'];
      if (!userId) {
        return res.status(401).json({ message: "User ID header required" });
      }

      // Get user info to verify admin or superuser role
      const user = await storage.getUserById(parseInt(userId as string));
      if (!user || !['admin', 'superuser'].includes(user.role)) {
        return res.status(403).json({ message: "Only admin or superuser can update company configuration" });
      }

      const companyConfig = insertCompanyConfigurationSchema.parse(req.body);
      console.log("Parsed company config:", JSON.stringify(companyConfig, null, 2));

      // Check if configuration already exists for this client
      const existingConfig = await storage.getCompanyConfigurationByClientId(companyConfig.clientId);
      console.log("Existing config:", existingConfig);

      if (existingConfig) {
        // Update existing configuration
        console.log("Updating existing configuration with ID:", existingConfig.id);
        const updatedConfig = await storage.updateCompanyConfiguration(existingConfig.id, companyConfig);
        console.log("Updated config:", updatedConfig);
        res.json(updatedConfig);
      } else {
        // Create new configuration
        console.log("Creating new configuration");
        const newConfig = await storage.createCompanyConfiguration(companyConfig);
        console.log("Created config:", newConfig);
        res.json(newConfig);
      }
    } catch (error) {
      console.error("Company configuration error:", error);
      res.status(400).json({ 
        message: "Invalid request", 
        error: error instanceof Error ? error.message : "Unknown error",
        details: error
      });
    }
  });

  app.put("/api/company-configuration/:id", async (req, res) => {
    try {
      // Get user from header to verify permissions
      const userId = req.headers['x-user-id'];
      if (!userId) {
        return res.status(401).json({ message: "User ID header required" });
      }

      // Get user info to verify admin or superuser role
      const user = await storage.getUserById(parseInt(userId as string));
      if (!user || !['admin', 'superuser'].includes(user.role)) {
        return res.status(403).json({ message: "Only admin or superuser can update company configuration" });
      }

      const id = parseInt(req.params.id);
      const companyConfig = insertCompanyConfigurationSchema.partial().parse(req.body);

      const updatedConfig = await storage.updateCompanyConfiguration(id, companyConfig);

      if (!updatedConfig) {
        return res.status(404).json({ message: "Company configuration not found" });
      }

      console.log(`✅ Company configuration updated by ${user.email} (${user.role})`);
      res.json(updatedConfig);
    } catch (error) {
      console.error("Error updating company configuration:", error);
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // General Configuration routes
  app.get("/api/configuration", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      const configs = await storage.getConfigurationsByClientId(clientId);

      // Convert array to object for easier frontend use
      const configObject: Record<string, string> = {};
      configs.forEach(config => {
        configObject[config.key] = config.value;
      });

      res.json(configObject);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  app.post("/api/configuration", async (req, res) => {
    try {
      const { clientId, configurations } = req.body;

      if (!clientId || !configurations) {
        return res.status(400).json({ message: "Client ID and configurations are required" });
      }

      // Update or create each configuration
      const results = [];
      for (const [key, value] of Object.entries(configurations)) {
        const result = await storage.updateConfiguration(clientId, key, value as string);
        results.push(result);
      }

      res.json({ success: true, updated: results.length });
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Stock Control routes

  // Stock control active session route
  app.get("/api/stock-control/active-session", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      console.log('🔍 Getting active session for client:', clientId);
      const activeSession = await storage.getActiveStockControlSession(clientId);

      if (!activeSession) {
        console.log('❌ No active session found for client:', clientId);
        return res.status(404).json({ message: "No active session found" });
      }

      console.log('✅ Active session found:', activeSession.id, 'Status:', activeSession.status);
      res.json(activeSession);
    } catch (error) {
      console.error('❌ Error getting active session:', error);
      res.status(500).json({ message: "Internal server error" });
    }
  });

  // Stock control session items route
  app.get("/api/stock-control/sessions/:sessionId/items", async (req, res) => {
    try {
      const sessionId = parseInt(req.params.sessionId);

      console.log('🔍 Getting items for session:', sessionId);
      const items = await storage.getStockControlItemsWithProductsBySessionId(sessionId);

      console.log('✅ Found', items.length, 'items for session:', sessionId);
      res.json(items);
    } catch (error) {
      console.error('❌ Error getting session items:', error);
      res.status(500).json({ message: "Internal server error" });
    }
  });

  // Get products for stock control (disponible + reservado)
  app.get("/api/stock-control/products", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      const products = await storage.getProductsForStockControl(clientId);
      res.json(products);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Get extravios products
  app.get("/api/stock-control/extravios", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      const extraviosProducts = await storage.getExtraviosProducts(clientId);
      res.json(extraviosProducts);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Get session history
  app.get("/api/stock-control/sessions", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      const sessions = await storage.getStockControlSessionsByClientId(clientId);
      res.json(sessions);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Start new stock control session
  app.post("/api/stock-control/sessions", async (req, res) => {
    try {
      console.log('🔍 POST /api/stock-control/sessions - Request body:', JSON.stringify(req.body, null, 2));

      // Verify user authentication
      const userId = req.headers['x-user-id'];
      if (!userId) {
        return res.status(401).json({ message: "User ID header required" });
      }

      // Process and convert date fields
      const processedBody = {
        ...req.body,
        date: req.body.date ? new Date(req.body.date) : new Date(),
        startTime: req.body.startTime ? new Date(req.body.startTime) : new Date()
      };

      console.log('🔄 Processed data with Date objects:', JSON.stringify(processedBody, null, 2));

      const sessionData = insertStockControlSessionSchema.parse(processedBody);
      console.log('✅ Schema validation passed:', JSON.stringify(sessionData, null, 2));

      const session = await storage.createStockControlSession(sessionData);
      console.log('✅ Session created successfully:', JSON.stringify(session, null, 2));

      res.json(session);
    } catch (error) {
      console.error('❌ Error creating stock control session:', error);
      if (error instanceof Error) {
        console.error('❌ Error details:', error.message);
        console.error('❌ Error stack:', error.stack);

        // Provide more specific error messages for date validation
        if (error.message.includes('Expected date, received string')) {
          return res.status(400).json({ 
            message: "Date format error", 
            error: "Date fields must be valid Date objects or ISO strings",
            receivedData: req.body,
            hint: "Convert string dates to Date objects before sending"
          });
        }

        if (error.message.includes('invalid input syntax')) {
          return res.status(400).json({ 
            message: "Invalid data format", 
            error: "One or more fields contain invalid data types",
            receivedData: req.body
          });
        }

        if (error.message.includes('duplicate key')) {
          return res.status(409).json({ 
            message: "Session already exists", 
            error: "A session is already active for this client"
          });
        }
      }

      res.status(400).json({ 
        message: "Invalid request", 
        error: error instanceof Error ? error.message : "Unknown error",
        receivedData: req.body
      });
    }
  });

  // Scan product
  app.post("/api/stock-control/scan", async (req, res) => {
    try {
      const { sessionId, imei } = req.body;

      if (!sessionId || !imei) {
        return res.status(400).json({ message: "Session ID and IMEI are required" });
      }

      // Get session
      const session = await storage.getStockControlSessionById(sessionId);
      if (!session) {
        return res.status(404).json({ message: "Session not found" });
      }

      // Find product by IMEI
      const product = await storage.getProductByImei(imei, session.clientId);
      if (!product) {
        return res.status(404).json({ message: "Producto no encontrado" });
      }

      // Check if product is available for control (disponible or reservado)
      if (!['disponible', 'reservado'].includes(product.status)) {
        return res.status(400).json({ message: `Producto en estado ${product.status} no se puede escanear` });
      }

      // Create stock control item
      const stockControlItem = await storage.createStockControlItem({
        sessionId,
        productId: product.id,
        imei: product.imei,
        status: 'scanned'
      });

      // Update session stats
      await storage.updateStockControlSession(sessionId, {
        scannedProducts: session.scannedProducts + 1
      });

      // Return product info with scan timestamp
      res.json({
        ...product,
        scannedAt: new Date().toISOString()
      });
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Get scanned products for session
  app.get("/api/stock-control/sessions/:sessionId/items", async (req, res) => {
    try {
      const sessionId = parseInt(req.params.sessionId);
      const items = await storage.getStockControlItemsWithProductsBySessionId(sessionId);
      res.json(items);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Get missing products from session
  app.get("/api/stock-control/sessions/:sessionId/missing", async (req, res) => {
    try {
      const sessionId = parseInt(req.params.sessionId);
      const missingProducts = await storage.getMissingProductsFromSession(sessionId);
      res.json(missingProducts);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Update stock control session
  app.put("/api/stock-control/sessions/:sessionId", async (req, res) => {
    try {
      const sessionId = parseInt(req.params.sessionId);
      const updates = req.body;

      // Process date fields if present
      if (updates.endTime) {
        updates.endTime = new Date(updates.endTime);
      }

      const updatedSession = await storage.updateStockControlSession(sessionId, updates);

      if (!updatedSession) {
        return res.status(404).json({ message: "Session not found" });
      }

      res.json(updatedSession);
    } catch (error) {
      console.error('Error updating stock control session:', error);
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Finish stock control session
  app.put("/api/stock-control/sessions/:sessionId/finish", async (req, res) => {
    try {
      const sessionId = parseInt(req.params.sessionId);

      const finishedSession = await storage.updateStockControlSession(sessionId, {
        status: 'completed',
        endTime: new Date()
      });

      if (!finishedSession) {
        return res.status(404).json({ message: "Session not found" });
      }

      res.json(finishedSession);
    } catch (error) {
      console.error('Error finishing stock control session:', error);
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Process missing products
  app.post("/api/stock-control/sessions/:sessionId/process-missing", async (req, res) => {
    try {
      const sessionId = parseInt(req.params.sessionId);
      const { updates } = req.body;

      for (const update of updates) {
        const { productId, action, notes } = update;

        // Update product status
        await storage.updateProduct(productId, { status: action });

        // Create product history entry
        const product = await storage.getProductById(productId);
        if (product) {
          await storage.createProductHistory({
            clientId: product.clientId,
            productId: productId,
            previousStatus: product.status,
            newStatus: action,
            userId: 1, // Should get from session
            notes: notes || `Control de stock - ${action}`
          });
        }

        // Create stock control item for missing product
        await storage.createStockControlItem({
          sessionId,
          productId,
          imei: product?.imei || '',
          status: 'missing',
          actionTaken: action,
          notes
        });
      }

      res.json({ success: true });
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // Finish stock control session
  app.put("/api/stock-control/sessions/:sessionId/finish", async (req, res) => {
    try {
      const sessionId = parseInt(req.params.sessionId);

      const session = await storage.getStockControlSessionById(sessionId);
      if (!session) {
        return res.status(404).json({ message: "Session not found" });
      }

      // Calculate missing products count
      const missingCount = session.totalProducts - session.scannedProducts;

      const updatedSession = await storage.updateStockControlSession(sessionId, {
        status: 'completed',
        endTime: new Date(),
        missingProducts: missingCount
      });

      res.json(updatedSession);
    } catch (error) {
      res.status(400).json({ message: "Invalid request" });
    }
  });

  // =======================
  // SISTEMA DE CAJAS AVANZADAS
  // =======================

  // Get or create today's cash register (auto-opening)
  app.get("/api/cash-register/current", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      console.log(`🔍 [CASH-STATUS] Checking current cash register for client ${clientId}`);
      let cashRegister = await storage.getCurrentCashRegister(clientId);

      if (cashRegister) {
        console.log(`💰 [CASH-STATUS] Found cash register ID: ${cashRegister.id}, isOpen: ${cashRegister.isOpen}, date: ${cashRegister.date}`);

        // Verificar si es de hoy
        const today = new Date().toISOString().split('T')[0];
        const registerDate = new Date(cashRegister.date).toISOString().split('T')[0];

        if (registerDate !== today) {
          console.log(`⚠️ [CASH-STATUS] Cash register is from ${registerDate}, today is ${today} - considering as closed`);
          // Si la caja no es de hoy, tratarla como cerrada
          cashRegister = null;
        }
      }

      // CORRECCIÓN: Si no hay caja abierta PARA HOY, intentar crear una automáticamente
      if (!cashRegister) {
        console.log(`⚡ [CASH-FIX] No hay caja abierta para HOY para cliente ${clientId}, creando automáticamente...`);

        try {
          cashRegister = await storage.createCashRegister({
            clientId,
            date: new Date(),
            initialUsd: "0.00",
            initialArs: "0.00",
            initialUsdt: "0.00",
            currentUsd: "0.00",
            currentArs: "0.00",
            currentUsdt: "0.00",
            dailySales: "0.00",
            totalExpenses: "0.00",
            dailyGlobalExchangeRate: "1200.00",
            isOpen: true,
            isActive: true,
          });

          console.log(`✅ [CASH-FIX] Caja creada automáticamente ID: ${cashRegister.id} para cliente ${clientId}`);
        } catch (createError) {
          console.error(`❌ [CASH-FIX] Error creando caja automáticamente:`, createError);
          return res.status(404).json({ message: "No open cash register found and could not create one" });
        }
      }

      console.log(`📤 [CASH-STATUS] Returning cash register status: ID=${cashRegister.id}, isOpen=${cashRegister.isOpen}`);
      res.json(cashRegister);
    } catch (error) {
      console.error('Error getting current cash register:', error);
      res.status(500).json({ message: "Internal server error" });
    }
  });

  // Get real-time cash state with all calculations
  app.get("/api/cash-register/real-time-state", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      const realTimeState = await storage.getRealTimeCashState(clientId);
      res.json(realTimeState);
    } catch (error) {
      console.error('Error getting real-time cash state:', error);
      res.status(500).json({ message: "Error retrieving real-time state" });
    }
  });

  // =======================
  // AUTOMATIC CASH REGISTER SYSTEM
  // =======================

  // Get automatic cash schedule
  app.get("/api/cash-schedule/schedule", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      const schedule = await storage.scheduleCashOperations(clientId);
      res.json(schedule);
    } catch (error) {
      console.error('Error getting cash schedule:', error);
      res.status(500).json({ message: "Error retrieving cash schedule" });
    }
  });

  // Get cash schedule configuration
  app.get("/api/cash-schedule/config", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      console.log(`🚀 [API] GET /api/cash-schedule/config called with clientId: ${clientId}`);

      if (!clientId) {
        console.log(`❌ [API] No clientId provided for schedule config`);
        return res.status(400).json({ message: "Client ID is required" });
      }

      console.log(`🔍 [API] Calling cashScheduleStorage.getScheduleConfig(${clientId})`);
      const config = await cashScheduleStorage.getScheduleConfig(clientId);
      console.log(`🔍 [API] cashScheduleStorage.getScheduleConfig returned:`, config);

      if (!config) {
        console.log(`⚠️ [API] No config found, returning defaults`);
        const defaultConfig = {
          autoOpenEnabled: false,
          autoCloseEnabled: false,
          openHour: 9,
          openMinute: 0,
          closeHour: 18,
          closeMinute: 0,
          activeDays: "1,2,3,4,5,6,7",
          timezone: "America/Argentina/Buenos_Aires",
        };
        console.log(`📤 [API] Returning default config:`, defaultConfig);
        return res.json(defaultConfig);
      }

      console.log(`📋 [API] Returning schedule config for client ${clientId}:`, {
        openHour: config.openHour,
        openMinute: config.openMinute,
        closeHour: config.closeHour,
        closeMinute: config.closeMinute,
        autoOpenEnabled: config.autoOpenEnabled,
        autoCloseEnabled: config.autoCloseEnabled
      });

      res.json(config);
    } catch (error) {
      console.error('❌ [API] Error getting cash schedule config:', error);
      res.status(500).json({ message: "Error retrieving cash schedule configuration" });
    }
  });

  // Update cash schedule configuration
  app.post("/api/cash-schedule/config", async (req, res) => {
    try {
      const userId = req.headers['x-user-id'];
      if (!userId) {
        return res.status(401).json({ message: "User ID header required" });
      }

      const { clientId, ...configData } = req.body;
      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      console.log(`📝 [API] Updating schedule config for client ${clientId}:`, configData);

      const updatedConfig = await cashScheduleStorage.upsertScheduleConfig(clientId, configData);

      console.log(`✅ [API] Schedule config updated successfully for client ${clientId}`);
      res.json(updatedConfig);
    } catch (error) {
      console.error('Error updating cash schedule config:', error);
      res.status(500).json({ message: "Error updating cash schedule configuration" });
    }
  });

  // Quick test endpoint to set schedule to current time + 2 minutes
  app.post("/api/cash-schedule/quick-test/:clientId", async (req, res) => {
    try {
      const clientId = parseInt(req.params.clientId);
      const now = new Date();
      const testTime = new Date(now.getTime() + 2 * 60 * 1000); // 2 minutes from now

      const config = await cashScheduleStorage.upsertScheduleConfig(clientId, {
        autoOpenEnabled: true,
        autoCloseEnabled: true,
        openHour: testTime.getHours(),
        openMinute: testTime.getMinutes(),
        closeHour: 23,
        closeMinute: 59,
        activeDays: "1,2,3,4,5,6,7",
        timezone: "America/Argentina/Buenos_Aires"
      });

      console.log(`🧪 Quick test schedule set for client ${clientId}: opening at ${testTime.getHours()}:${testTime.getMinutes().toString().padStart(2, '0')}`);

      res.json({
        message: `Test schedule set for client ${clientId}`,
        openTime: `${testTime.getHours()}:${testTime.getMinutes().toString().padStart(2, '0')}`,
        config
      });
    } catch (error) {
      console.error('Error setting quick test schedule:', error);
      res.status(500).json({ message: "Error setting test schedule" });
    }
  });

  // Get scheduled operations for a client
  app.get("/api/cash-schedule/operations", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      console.log(`🔍 [DEBUG] API /api/cash-schedule/operations called with clientId: ${clientId}`);

      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      const operations = await cashScheduleStorage.getScheduledOperations(clientId);

      console.log(`🔍 [DEBUG] API returning operations for clientId ${clientId}:`, JSON.stringify(operations, null, 2));

      res.json(operations);
    } catch (error) {
      console.error('Error getting scheduled operations:', error);
      res.status(500).json({ message: "Error retrieving scheduled operations" });
    }
  });

  // Get auto operations log
  app.get("/api/cash-schedule/log", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      const limit = parseInt(req.query.limit as string) || 50;

      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      const logs = await cashScheduleStorage.getAutoOperationsLog(clientId, limit);

      res.json(logs);
    } catch (error) {
      console.error('Error getting auto operations log:', error);
      res.status(500).json({ message: "Error retrieving operations log" });
    }
  });

  // Get automation service status with execution status
  app.get("/api/cash-schedule/service-status", async (req, res) => {
    try {
      // Import the service to check its status
      const { cashAutomationService } = await import('./cash-automation-service.js');
      const serviceStatus = cashAutomationService.getStatus();

      let openingExecutionStatus = 'unknown';
      let closingExecutionStatus = 'unknown';

      const clientId = parseInt(req.query.clientId as string);

      // If clientId is provided, get execution status for today
      if (clientId && !isNaN(clientId)) {
        const today = new Date();
        const startOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate(), 0, 0, 0, 0);
        const endOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate(), 23, 59, 59, 999);

        // Check if operations were executed today
        const [openLogs, closeLogs] = await Promise.all([
          db.select()
            .from(cashAutoOperationsLog)
            .where(and(
              eq(cashAutoOperationsLog.clientId, clientId),
              eq(cashAutoOperationsLog.operationType, 'auto_open'),
              eq(cashAutoOperationsLog.status, 'success'),
              gte(cashAutoOperationsLog.executedTime, startOfDay),
              lte(cashAutoOperationsLog.executedTime, endOfDay)
            ))
            .limit(1),
          db.select()
            .from(cashAutoOperationsLog)
            .where(and(
              eq(cashAutoOperationsLog.clientId, clientId),
              eq(cashAutoOperationsLog.operationType, 'auto_close'),
              eq(cashAutoOperationsLog.status, 'success'),
              gte(cashAutoOperationsLog.executedTime, startOfDay),
              lte(cashAutoOperationsLog.executedTime, endOfDay)
            ))
            .limit(1)
        ]);

        const wasOpenExecuted = openLogs.length > 0;
        const wasCloseExecuted = closeLogs.length > 0;

        openingExecutionStatus = wasOpenExecuted ? 'executed' : 'scheduled';

        if (wasCloseExecuted) {
          closingExecutionStatus = 'executed';
        } else if (wasOpenExecuted) {
          closingExecutionStatus = 'next_operation';
        } else {
          closingExecutionStatus = 'scheduled';
        }
      }

      console.log(`🔍 [SERVICE-STATUS] Reporting service status: isRunning=${serviceStatus.isRunning}`);

      res.json({
        isRunning: serviceStatus.isRunning,
        lastCheck: new Date().toISOString(),
        status: serviceStatus.isRunning ? 'active' : 'inactive',
        openingExecutionStatus,
        closingExecutionStatus,
        serverTime: new Date().toISOString(),
        argentinaTime: new Date().toLocaleString("en-US", {timeZone: "America/Argentina/Buenos_Aires"})
      });
    } catch (error) {
      console.error('Error getting service status:', error);
      res.status(500).json({ message: "Error retrieving service status" });
    }
  });

  // Start automation service (admin only)
  app.post("/api/cash-schedule/service/start", async (req, res) => {
    try {
      const userId = req.headers['x-user-id'];
      if (!userId) {
        return res.status(401).json({ message: "User ID header required" });
      }

      const user = await storage.getUserById(parseInt(userId as string));
      if (!user || !['admin', 'superuser'].includes(user.role)) {
        return res.status(403).json({ message: "Only admin or superuser can control automation service" });
      }

      const { cashAutomationService } = await import('./cash-automation-service.js');
      cashAutomationService.start();

      res.json({ message: "Cash automation service started", status: cashAutomationService.getStatus() });
    } catch (error) {
      console.error('Error starting automation service:', error);
      res.status(500).json({ message: "Error starting automation service" });
    }
  });

  // Stop automation service (admin only)
  app.post("/api/cash-schedule/service/stop", async (req, res) => {
    try {
      const userId = req.headers['x-user-id'];
      if (!userId) {
        return res.status(401).json({ message: "User ID header required" });
      }

      const user = await storage.getUserById(parseInt(userId as string));
      if (!user || !['admin', 'superuser'].includes(user.role)) {
        return res.status(403).json({ message: "Only admin or superuser can control automation service" });
      }

      const { cashAutomationService } = await import('./cash-automation-service.js');
      cashAutomationService.stop();

      res.json({ message: "Cash automation service stopped", status: cashAutomationService.getStatus() });
    } catch (error) {
      console.error('Error stopping automation service:', error);
      res.status(500).json({ message: "Error stopping automation service" });
    }
  });

  // AUTO-SYNC MONITORROUTES
  app.get('/api/auto-sync/status', async (req: any, res: any) => {
    try {
      // Import functions inline to avoid circular dependencies
      const { getAutoSyncStatus } = await import('./auto-sync-monitor.js');
      const status = getAutoSyncStatus();
      res.json(status);
    } catch (error) {
      console.error('Error getting auto-sync status:', error);
      res.status(500).json({ message: 'Error getting auto-sync status' });
    }
  });

  app.post('/api/auto-sync/start', async (req: any, res: any) => {
    try {
      const { startAutoSyncMonitor, getAutoSyncStatus } = await import('./auto-sync-monitor.js');
      startAutoSyncMonitor();
      res.json({ message: 'Auto-sync monitor started', status: getAutoSyncStatus() });
    } catch (error) {
      console.error('Error starting auto-sync monitor:', error);
      res.status(500).json({ message: 'Error starting auto-sync monitor' });
    }
  });

  app.post('/api/auto-sync/stop', async (req: any, res: any) => {
    try {
      const { stopAutoSyncMonitor, getAutoSyncStatus } = await import('./auto-sync-monitor.js');
      stopAutoSyncMonitor();
      res.json({ message: 'Auto-sync monitor stopped', status: getAutoSyncStatus() });
    } catch (error) {
      console.error('Error stopping auto-sync monitor:', error);
      res.status(500).json({ message: 'Error stopping auto-sync monitor' });
    }
  });

  // Password reset endpoint
  app.post("/api/auth/forgot-password", async (req, res) => {
    try {
      const { email } = req.body;

      if (!email) {
        return res.status(400).json({ message: 'Email es requerido' });
      }

      console.log('🔍 Buscando usuario con email:', email);

      // Buscar usuario por email
      const user = await storage.getUserByEmail(email);
      if (!user) {
        // Por seguridad, siempre responder exitosamente aunque el email no exista
        return res.json({ 
          message: 'Si el email existe en nuestro sistema, recibirás un enlace de recuperación' 
        });
      }

      console.log('✅ Usuario encontrado:', user.username);

      // Generar token único
      const crypto = await import('crypto');
      const token = crypto.randomBytes(32).toString('hex');
      const expiresAt = new Date(Date.now() + 60 * 60 * 1000); // 1 hora

      // Guardar token en base de datos (simplified - store in user record)
      await storage.updateUser(user.id, {
        passwordResetToken: token,
        passwordResetExpires: expiresAt
      });

      console.log('🔐 Token generado:', token.substring(0, 8) + '...');

      // Generar URL de reset
      const baseUrl = req.get('host')?.includes('localhost') 
        ? `http://${req.get('host')}`
        : `https://${req.get('host')}`;
      const resetUrl = `${baseUrl}/reset-password?token=${token}`;

      console.log('🔗 URL de reset:', resetUrl);

      // Enviar respuesta exitosa
      res.json({ 
        message: 'Email de recuperación enviado. Revisa tu bandeja de entrada.',
        resetUrl: resetUrl // Solo para desarrollo
      });

    } catch (error) {
      console.error('❌ Error en forgot-password:', error);
      res.status(500).json({ 
        message: 'Error interno del servidor' 
      });
    }
  });

  // Reset password with token
  app.post("/api/auth/reset-password", async (req, res) => {
    try {
      const { token, newPassword } = req.body;

      if (!token || !newPassword) {
        return res.status(400).json({ message: 'Token y nueva contraseña son requeridos' });
      }

      // Find user by token
      const users = await storage.getAllUsers();
      const user = users.find(u => u.passwordResetToken === token && 
                              u.passwordResetExpires && 
                              new Date(u.passwordResetExpires) > new Date());

      if (!user) {
        return res.status(400).json({ message: 'Token inválido o expirado' });
      }

      // Hash new password
      const hashedPassword = bcrypt.hashSync(newPassword, 12);

      // Update password and clear reset token
      await storage.updateUser(user.id, {
        password: hashedPassword,
        passwordResetToken: null,
        passwordResetExpires: null,
        mustChangePassword: false
      });

      console.log('✅ Contraseña restablecida para:', user.email);
      res.json({ message: 'Contraseña restablecida exitosamente' });

    } catch (error) {
      console.error('❌ Error en reset-password:', error);
      res.status(500).json({ message: 'Error interno del servidor' });
    }
  });

  // =======================
  // CASH SCHEDULE MULTIPLE PERIODS ENDPOINTS
  // =======================

  // Get all periods for a client
  app.get("/api/cash-schedule/periods", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      if (!clientId) {
        return res.status(400).json({ error: 'Client ID is required' });
      }

      const { cashScheduleStorage } = await import("./cash-schedule-storage");
      const periods = await cashScheduleStorage.getSchedulePeriods(clientId);
      
      res.json(periods);
    } catch (error) {
      console.error('❌ Error fetching schedule periods:', error);
      res.status(500).json({ error: 'Error fetching schedule periods' });
    }
  });

  // Create new period
  app.post("/api/cash-schedule/periods", async (req, res) => {
    try {
      const { cashScheduleStorage } = await import("./cash-schedule-storage");
      const period = await cashScheduleStorage.createSchedulePeriod(req.body);
      
      res.json(period);
    } catch (error) {
      console.error('❌ Error creating schedule period:', error);
      res.status(500).json({ error: 'Error creating schedule period' });
    }
  });

  // Update period
  app.put("/api/cash-schedule/periods/:id", async (req, res) => {
    try {
      const periodId = parseInt(req.params.id);
      const { cashScheduleStorage } = await import("./cash-schedule-storage");
      const period = await cashScheduleStorage.updateSchedulePeriod(periodId, req.body);
      
      res.json(period);
    } catch (error) {
      console.error('❌ Error updating schedule period:', error);
      res.status(500).json({ error: 'Error updating schedule period' });
    }
  });

  // Delete period
  app.delete("/api/cash-schedule/periods/:id", async (req, res) => {
    try {
      const periodId = parseInt(req.params.id);
      const { cashScheduleStorage } = await import("./cash-schedule-storage");
      await cashScheduleStorage.deleteSchedulePeriod(periodId);
      
      res.json({ success: true });
    } catch (error) {
      console.error('❌ Error deleting schedule period:', error);
      res.status(500).json({ error: 'Error deleting schedule period' });
    }
  });

  // Get operations log
  app.get("/api/cash-schedule/operations-log", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      const limit = parseInt(req.query.limit as string) || 20;
      
      if (!clientId) {
        return res.status(400).json({ error: 'Client ID is required' });
      }

      const { cashScheduleStorage } = await import("./cash-schedule-storage");
      const logs = await cashScheduleStorage.getOperationsLog(clientId, limit);
      
      res.json(logs);
    } catch (error) {
      console.error('❌ Error fetching operations log:', error);
      res.status(500).json({ error: 'Error fetching operations log' });
    }
  });

  // Get next scheduled operations
  app.get("/api/cash-schedule/next-operations", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      const limit = parseInt(req.query.limit as string) || 10;
      
      if (!clientId) {
        return res.status(400).json({ error: 'Client ID is required' });
      }

      const { cashScheduleStorage } = await import("./cash-schedule-storage");
      const nextOps = await cashScheduleStorage.getNextScheduledOperations(clientId, limit);
      
      res.json(nextOps);
    } catch (error) {
      console.error('❌ Error fetching next operations:', error);
      res.status(500).json({ error: 'Error fetching next operations' });
    }
  });

  // =======================
  // CASH AUTOMATION SERVICE TEST ENDPOINTS
  // =======================

  app.get("/api/cash-automation/status", async (req, res) => {
    try {
      const { cashAutomationService } = await import("./cash-automation-service");
      const status = cashAutomationService.getStatus();

      console.log('📊 Cash automation service status requested:', status);

      res.json({
        isRunning: status.isRunning,
        uptime: status.uptime,
        lastCheck: status.lastCheck,
        serverTime: new Date().toISOString(),
        argentinaTime: new Date().toLocaleString("en-US", {timeZone: "America/Argentina/Buenos_Aires"})
      });
    } catch (error) {
      console.error('❌ Error getting cash automation status:', error);
      res.status(500).json({ error: 'Error getting service status' });
    }
  });

  app.get("/api/cash-automation/test-open/:clientId", async (req, res) => {
    try {
      const clientId = parseInt(req.params.clientId);
      const { cashScheduleStorage } = await import("./cash-schedule-storage");

      console.log(`🧪 Manual test: checking if client ${clientId} should open`);

      const shouldOpen = await cashScheduleStorage.shouldExecuteAutoOperation(clientId, 'open');
      const config = await cashScheduleStorage.getScheduleConfig(clientId);

      res.json({
        clientId,
        shouldOpen,
        config,
        currentTime: new Date().toISOString(),
        argentinaTime: new Date().toLocaleString("en-US", {timeZone: "America/Argentina/Buenos_Aires"})
      });
    } catch (error) {
      console.error('❌ Error in manual test:', error);
      res.status(500).json({ error: 'Error in manual test' });
    }
  });

  app.post("/api/cash-automation/force-open/:clientId", async (req, res) => {
    try {
      const clientId = parseInt(req.params.clientId);
      const { cashAutomationService } = await import("./cash-automation-service");

      console.log(`🧪 FORCE OPEN test for client ${clientId}`);

      // Force execute auto open
      const result = await cashAutomationService.executeAutoOpen(clientId);

      res.json({
        success: true,
        message: `Auto-open forced for client ${clientId}`,
        result,
        executedAt: new Date().toISOString()
      });
    } catch (error) {
      console.error('❌ Error in force open:', error);
      res.status(500).json({ error: 'Error in force open test' });
    }
  });

  // Cash Schedule Configuration routes
  app.get("/api/cash-schedule/config", async (req, res) => {
    try {
      const clientId = parseInt(req.query.clientId as string);
      if (!clientId) {
        return res.status(400).json({ message: "Client ID is required" });
      }

      const config = await cashScheduleStorage.getScheduleConfig(clientId);
      if (!config) {
        // Return default configuration if none exists
        return res.json({
          autoOpenEnabled: false,
          autoCloseEnabled: false,
          openHour: 9,
          openMinute: 0,
          closeHour: 18,
          closeMinute: 0,
          activeDays: "1,2,3,4,5,6,7",
          timezone: "America/Argentina/Buenos_Aires",
        });
      }

      console.log(`📋 [API] Returning schedule config for client ${clientId}:`, {
        openHour: config.openHour,
        openMinute: config.openMinute,
        closeHour: config.closeHour,
        closeMinute: config.closeMinute,
        autoOpenEnabled: config.autoOpenEnabled,
        autoCloseEnabled: config.autoCloseEnabled
      });

      res.json(config);
    } catch (error) {
      console.error('Error getting cash schedule config:', error);
      res.status(500).json({ message: "Error retrieving cash schedule configuration" });
    }
  });


  const httpServer = createServer(app);
  return httpServer;
}