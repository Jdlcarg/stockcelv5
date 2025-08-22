import { useState, useEffect, useRef } from "react";
import { useLocation } from "wouter";
import { useAuth } from "@/lib/auth";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { apiRequest } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import Header from "@/components/layout/header";
import Sidebar from "@/components/layout/sidebar";
import MobileNav from "@/components/layout/mobile-nav";
import Footer from "@/components/layout/footer";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { ArrowLeft, Scan, Trash2, ShoppingCart } from "lucide-react";

interface Product {
  id: number;
  imei: string;
  model: string;
  storage: string;
  color: string;
  costPrice: string;
  status: string;
  battery?: string;
  quality?: string;
}

interface OrderItem {
  productId: number;
  product: Product;
  salePrice: string;
  imei: string;
  model: string;
  color: string;
  storage: string;
  status: string;
  costPrice: string;
  battery?: string;
  quality?: string;
}

interface Customer {
  id: number;
  name: string;
  phone?: string;
  email?: string;
  dni?: string;
}

interface Vendor {
  id: number;
  name: string;
  commissionPercentage: number;
}

export default function CreateOrder() {
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const [, setLocation] = useLocation();
  const { isAuthenticated, user } = useAuth();
  const { toast } = useToast();
  const queryClient = useQueryClient();
  
  // Estados del escaneo
  const [imeiInput, setImeiInput] = useState("");
  const [scannedProducts, setScannedProducts] = useState<OrderItem[]>([]);
  const [defaultPrice, setDefaultPrice] = useState("");
  const [orderNotes, setOrderNotes] = useState("");
  const imeiInputRef = useRef<HTMLInputElement>(null);
  
  // Estados del cliente
  const [customerName, setCustomerName] = useState("");
  const [customerLastName, setCustomerLastName] = useState("");
  
  // Estados para el cuadro flotante arrastrable
  const [isDragging, setIsDragging] = useState(false);
  const [dragOffset, setDragOffset] = useState({ x: 0, y: 0 });
  const [position, setPosition] = useState({ x: 0, y: 0 });
  const [customerDni, setCustomerDni] = useState("");
  const [selectedCustomerId, setSelectedCustomerId] = useState("");
  const [selectedVendorId, setSelectedVendorId] = useState("");
  
  // Estados de entrega
  const [deliveryMethod, setDeliveryMethod] = useState<"oficina" | "direccion" | "">("");
  const [deliveryAddress, setDeliveryAddress] = useState("");
  
  // Estados del pago
  const [paymentStatus, setPaymentStatus] = useState<"pagado" | "parcial" | "no_pagado" | "">("");
  const [paymentMethods, setPaymentMethods] = useState<string[]>([]);
  const [paymentAmounts, setPaymentAmounts] = useState<{[key: string]: string}>({});
  const [customPaymentMethod, setCustomPaymentMethod] = useState("");
  
  // Estados para tipos de cambio específicos por método de pago
  const [paymentExchangeRates, setPaymentExchangeRates] = useState<{[key: string]: string}>({
    'efectivo_ars': localStorage.getItem(`tc_efectivo_ars_${user?.clientId}`) || "1000", // ARS→USD
    'transferencia_ars': localStorage.getItem(`tc_transferencia_ars_${user?.clientId}`) || "1000", // ARS→USD
    'transferencia_usdt': localStorage.getItem(`tc_transferencia_usdt_${user?.clientId}`) || "1050", // USD→ARS  
    'efectivo_usd': localStorage.getItem(`tc_efectivo_usd_${user?.clientId}`) || "1050", // USD→ARS
    'transferencia_usd': localStorage.getItem(`tc_transferencia_usd_${user?.clientId}`) || "1050", // USD→ARS
    'financiera_usd_ars': localStorage.getItem(`tc_financiera_usd_ars_${user?.clientId}`) || "1100", // USD→ARS
    'financiera_ars_usd': localStorage.getItem(`tc_financiera_ars_usd_${user?.clientId}`) || "1050", // ARS→USD
  });
  
  // Estados especiales para financieras (mantener compatibilidad)
  const [financieraUSDAmount, setFinancieraUSDAmount] = useState("");
  const [financieraARSAmount, setFinancieraARSAmount] = useState("");
  const [exchangeRate, setExchangeRate] = useState(() => {
    return localStorage.getItem(`exchange_rate_ars_usd_${user?.clientId}`) || "1000";
  });

  useEffect(() => {
    if (!isAuthenticated) {
      setLocation("/");
    }
  }, [isAuthenticated, setLocation]);

  // Datos
  const { data: products } = useQuery({
    queryKey: ['/api/products', user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/products?clientId=${user?.clientId}`);
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  const { data: customers } = useQuery({
    queryKey: ['/api/customers', user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/customers?clientId=${user?.clientId}`);
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  const { data: vendors } = useQuery({
    queryKey: ['/api/vendors', user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/vendors?clientId=${user?.clientId}`);
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  // Obtener configuración del cliente para el tipo de cambio
  const { data: clientConfig } = useQuery({
    queryKey: ['/api/configuration', user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/configuration?clientId=${user?.clientId}`);
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  // Establecer los tipos de cambio desde la configuración o localStorage
  useEffect(() => {
    if (user?.clientId) {
      const savedRate = localStorage.getItem(`exchange_rate_ars_usd_${user.clientId}`);
      
      if (savedRate) {
        setExchangeRate(savedRate);
      } else if (clientConfig && clientConfig.exchangeRate) {
        setExchangeRate(clientConfig.exchangeRate);
      }
    }
  }, [clientConfig, user?.clientId]);

  // Función para actualizar tipo de cambio específico
  const updatePaymentExchangeRate = (paymentMethod: string, rate: string) => {
    setPaymentExchangeRates(prev => ({
      ...prev,
      [paymentMethod]: rate
    }));
    
    // Guardar en localStorage
    if (user?.clientId) {
      localStorage.setItem(`tc_${paymentMethod}_${user.clientId}`, rate);
    }
  };

  // Función para obtener el label del tipo de cambio según el método de pago
  const getExchangeRateLabel = (paymentMethod: string): string => {
    const labels: { [key: string]: string } = {
      'efectivo_ars': 'TC ARS→USD',
      'transferencia_ars': 'TC ARS→USD', 
      'transferencia_usdt': 'TC USD→ARS',
      'efectivo_usd': 'TC USD→ARS',
      'transferencia_usd': 'TC USD→ARS',
      'financiera_usd_ars': 'TC USD→ARS',
      'financiera_ars_usd': 'TC ARS→USD',
    };
    return labels[paymentMethod] || 'TC';
  };

  // Función para calcular ARS desde USD usando el tipo de cambio general
  const calculateARSFromUSD = (usdAmount: string): string => {
    if (!usdAmount || !exchangeRate) return "0.00";
    const usd = parseFloat(usdAmount);
    const rate = parseFloat(exchangeRate);
    if (isNaN(usd) || isNaN(rate)) return "0.00";
    return (usd * rate).toFixed(2);
  };

  // Función para calcular ARS desde USD usando la tasa de financiera USD
  const calculateARSFromUSDFinanciera = (usdAmount: string): string => {
    if (!usdAmount || !paymentExchangeRates['financiera_usd_ars']) return "0.00";
    const usd = parseFloat(usdAmount);
    const rate = parseFloat(paymentExchangeRates['financiera_usd_ars']);
    if (isNaN(usd) || isNaN(rate)) return "0.00";
    return (usd * rate).toFixed(2);
  };
  
  // Función para calcular USD desde ARS usando la tasa de financiera ARS
  const calculateUSDFromARSFinanciera = (arsAmount: string): string => {
    if (!arsAmount || !paymentExchangeRates['financiera_ars_usd']) return "0.00";
    const ars = parseFloat(arsAmount);
    const rate = parseFloat(paymentExchangeRates['financiera_ars_usd']);
    if (isNaN(ars) || isNaN(rate)) return "0.00";
    return (ars / rate).toFixed(2);
  };

  // Actualizar montos de financieras
  useEffect(() => {
    if (paymentMethods.includes('financiera_usd') && financieraUSDAmount) {
      setPaymentAmounts(prev => ({
        ...prev,
        financiera_usd: financieraUSDAmount
      }));
    }
    if (paymentMethods.includes('financiera_ars') && financieraARSAmount) {
      setPaymentAmounts(prev => ({
        ...prev,
        financiera_ars: financieraARSAmount
      }));
    }
  }, [financieraUSDAmount, financieraARSAmount, paymentMethods]);

  const availablePaymentMethods = [
    { value: "efectivo_ars", label: "Efectivo ARS", currency: "ARS" },
    { value: "efectivo_usd", label: "Efectivo USD", currency: "USD" },
    { value: "transferencia_ars", label: "Transferencia ARS", currency: "ARS" },
    { value: "transferencia_usd", label: "Transferencia USD", currency: "USD" },
    { value: "transferencia_usdt", label: "Transferencia USDT", currency: "USDT" },
    { value: "financiera_usd", label: "Financiera USD→ARS", currency: "USD" },
    { value: "financiera_ars", label: "Financiera ARS→USD", currency: "ARS" },
  ];

  // Auto-focus en el input de IMEI
  useEffect(() => {
    if (imeiInputRef.current) {
      imeiInputRef.current.focus();
    }
  }, []);

  const handleScanIMEI = async () => {
    if (!imeiInput.trim()) return;

    // Invalidar y refrescar la lista de productos primero
    await queryClient.invalidateQueries({ queryKey: ['/api/products', user?.clientId] });
    
    // Hacer una nueva consulta para obtener productos actualizados
    const response = await fetch(`/api/products?clientId=${user?.clientId}`);
    const updatedProducts = await response.json();

    if (!updatedProducts || !Array.isArray(updatedProducts)) {
      toast({
        title: "Error",
        description: "Error al cargar productos actualizados",
        variant: "destructive",
      });
      return;
    }

    const product = updatedProducts.find((p: Product) => p.imei === imeiInput.trim());
    
    if (!product) {
      toast({
        title: "Producto no encontrado",
        description: `No se encontró un producto con IMEI: ${imeiInput}`,
        variant: "destructive",
      });
      setImeiInput("");
      return;
    }

    // 🔥 NUEVA FUNCIONALIDAD: Productos reservados ahora permitidos para venta
    if (product.status === 'reservado') {
      toast({
        title: "🎉 RESERVADO PERMITIDO",
        description: `¡ACTUALIZACIÓN! Los productos reservados ahora se pueden vender normalmente`,
        duration: 3000,
      });
      // Continue with adding product - don't return
    } else if (product.status === 'disponible') {
      toast({
        title: "✅ Producto Disponible",
        description: `Producto agregado correctamente`,
        duration: 2000,
      });
      // Continue with adding product - don't return  
    } else {
      toast({
        title: "❌ Estado No Permitido",
        description: `Estado ${product.status} bloqueado. Solo DISPONIBLES y RESERVADOS están permitidos para la venta.`,
        variant: "destructive",
      });
      setImeiInput("");
      return;
    }

    // Verificar duplicados
    if (scannedProducts.some(item => item.productId === product.id)) {
      toast({
        title: "Producto duplicado",
        description: "Este producto ya está en el pedido",
        variant: "destructive",
      });
      setImeiInput("");
      return;
    }

    // Agregar producto al pedido con precio por defecto
    const newOrderItem: OrderItem = {
      productId: product.id,
      product,
      salePrice: defaultPrice || "",
      imei: product.imei,
      model: product.model,
      color: product.color,
      storage: product.storage,
      status: product.status,
      costPrice: product.costPrice,
      battery: product.battery,
      quality: product.quality,
    };

    setScannedProducts(prev => [...prev, newOrderItem]);
    setImeiInput("");
    
    toast({
      title: "Producto agregado",
      description: `${product.model} ${product.storage} agregado al pedido`,
    });

    // Auto-focus al campo de escaneo
    setTimeout(() => {
      if (imeiInputRef.current) {
        imeiInputRef.current.focus();
      }
    }, 100);
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleScanIMEI();
    }
  };

  const handlePriceChange = (index: number, price: string) => {
    setScannedProducts(prev => 
      prev.map((item, i) => 
        i === index 
          ? { ...item, salePrice: price }
          : item
      )
    );
  };

  const handlePriceKeyPress = (e: React.KeyboardEvent, index: number) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      const nextIndex = index + 1;
      if (nextIndex < scannedProducts.length) {
        const nextInput = document.querySelector(`input[data-price-index="${nextIndex}"]`) as HTMLInputElement;
        if (nextInput) {
          nextInput.focus();
        }
      } else {
        // Enfocar sección de pago
        const paymentSection = document.querySelector('[data-payment-section]') as HTMLElement;
        if (paymentSection) {
          paymentSection.scrollIntoView({ behavior: 'smooth' });
          paymentSection.focus();
        }
      }
    }
  };

  const removeProduct = (productId: number) => {
    setScannedProducts(prev => prev.filter(item => item.productId !== productId));
  };

  const clearScannedProducts = () => {
    setScannedProducts([]);
    toast({
      title: "Lista limpiada",
      description: "Se eliminaron todos los productos del pedido",
    });
  };

  const calculateTotal = () => {
    return scannedProducts.reduce((sum, item) => sum + (parseFloat(item.salePrice) || 0), 0);
  };

  const calculateTotalPaid = () => {
    let total = 0;
    paymentMethods.forEach(method => {
      let amount = 0;
      
      if (method === 'financiera_usd') {
        amount = parseFloat(financieraUSDAmount) || 0;
        total += amount; // USD directo
      } else if (method === 'financiera_ars') {
        const arsAmount = parseFloat(financieraARSAmount) || 0;
        const rate = parseFloat(paymentExchangeRates['financiera_ars_usd']) || 1050;
        total += arsAmount / rate; // Convertir ARS a USD
      } else {
        amount = parseFloat(paymentAmounts[method]) || 0;
        
        if (method === 'efectivo_ars' || method === 'transferencia_ars') {
          // Usar tipo de cambio específico para convertir ARS a USD
          const rate = parseFloat(paymentExchangeRates[method]) || 1000;
          total += amount / rate;
        } else if (method === 'efectivo_usd' || method === 'transferencia_usd' || method === 'transferencia_usdt') {
          // USD/USDT directo
          total += amount;
        }
      }
    });
    return total;
  };

  const createOrderMutation = useMutation({
    mutationFn: async () => {
      if (scannedProducts.length === 0) {
        throw new Error("Debe agregar al menos un producto al pedido");
      }

      if (!paymentStatus) {
        throw new Error("Debe seleccionar el estado del pago");
      }

      if (paymentMethods.length === 0) {
        throw new Error("Debe seleccionar al menos una forma de pago");
      }

      const totalAmount = calculateTotal();
      const orderNumber = `ORD-${Date.now()}`;
      
      // Determinar nombre del cliente
      let finalCustomerName = customerName.trim();
      if (selectedCustomerId) {
        const selectedCustomer = customers?.find((c: Customer) => c.id === parseInt(selectedCustomerId));
        finalCustomerName = selectedCustomer?.name || customerName.trim();
      }
      
      // Crear el pedido
      const orderResponse = await apiRequest('POST', '/api/orders', {
        clientId: user?.clientId,
        orderNumber,
        customerName: finalCustomerName,
        customerPhone: null,
        customerEmail: null,
        customerId: selectedCustomerId ? parseInt(selectedCustomerId) : null,
        totalUsd: totalAmount.toFixed(2),
        status: 'completado',
        paymentStatus: paymentStatus,
        vendorId: selectedVendorId ? parseInt(selectedVendorId) : user?.id,
        observations: orderNotes.trim() || null,
        shippingType: deliveryMethod || null,
        shippingAddress: deliveryMethod === 'direccion' ? deliveryAddress.trim() : null,
      });
      
      const order = await orderResponse.json();

      // Crear los items del pedido
      for (const item of scannedProducts) {
        const orderItemData = {
          orderId: order.insertedId || order.id,
          productId: item.productId,
          quantity: 1,
          priceUsd: item.salePrice,
          paymentMethod: paymentMethods.join(', '),
          exchangeRate: exchangeRate,
          amountUsd: item.salePrice,
        };
        
        console.log('Creating order item with data:', orderItemData);
        
        try {
          await apiRequest('POST', '/api/order-items', orderItemData);
        } catch (error) {
          console.error('Error creating order item:', error);
          throw error;
        }

        // Cambiar estado del producto a vendido
        await apiRequest('PUT', `/api/products/${item.productId}`, {
          status: 'vendido',
          userId: user?.id,
        });
      }

      // Crear registros de pagos
      for (const method of paymentMethods) {
        let amount = 0;
        let amountUsd = 0;
        let amountLocal = 0;
        let currency = 'USD';
        let rateToUse = '1';

        // Obtener el monto según el tipo de pago
        if (method === 'financiera_usd') {
          amount = parseFloat(financieraUSDAmount) || 0;
          amountUsd = amount; // Monto en USD directo
          const rate = parseFloat(paymentExchangeRates['financiera_usd_ars']) || 1100;
          amountLocal = amount * rate; // Convertir a ARS para registro
          currency = 'USD';
          rateToUse = paymentExchangeRates['financiera_usd_ars'];
        } else if (method === 'financiera_ars') {
          amount = parseFloat(financieraARSAmount) || 0;
          amountLocal = amount; // Monto en ARS directo
          const rate = parseFloat(paymentExchangeRates['financiera_ars_usd']) || 1050;
          amountUsd = amount / rate; // Convertir a USD
          currency = 'ARS';
          rateToUse = paymentExchangeRates['financiera_ars_usd'];
        } else {
          amount = parseFloat(paymentAmounts[method]) || 0;
          if (method === 'efectivo_ars' || method === 'transferencia_ars') {
            // Usar tipo de cambio específico del método
            const rate = parseFloat(paymentExchangeRates[method]) || 1000;
            amountUsd = amount / rate;
            amountLocal = amount;
            currency = 'ARS';
            rateToUse = paymentExchangeRates[method];
          } else if (method === 'efectivo_usd' || method === 'transferencia_usd') {
            // USD directo
            amountUsd = amount;
            amountLocal = amount;
            currency = 'USD';
            rateToUse = '1';
          } else if (method === 'transferencia_usdt') {
            // USDT - usar como USD pero con su propio tipo de cambio si aplica
            amountUsd = amount;
            amountLocal = amount;
            currency = 'USDT';
            rateToUse = '1';
          }
        }

        if (amount > 0) {
          console.log('Creating payment:', {
            clientId: user?.clientId,
            orderId: order.insertedId || order.id,
            paymentMethod: method,
            amount: amountLocal.toFixed(2),
            amountUsd: amountUsd.toFixed(2),
            exchangeRate: rateToUse,
            notes: `Pago ${method} - ${currency} ${amountLocal.toFixed(2)}`
          });
          
          await apiRequest('POST', '/api/payments', {
            clientId: user?.clientId,
            orderId: order.insertedId || order.id,
            paymentMethod: method,
            amount: amountLocal.toFixed(2),
            amountUsd: amountUsd.toFixed(2),
            exchangeRate: rateToUse,
            notes: `Pago ${method} - ${currency} ${amountLocal.toFixed(2)}`
          });
        }
      }

      return order;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/orders'] });
      queryClient.invalidateQueries({ queryKey: ['/api/products'] });
      
      toast({
        title: "Pedido creado exitosamente",
        description: `Se creó el pedido con ${scannedProducts.length} productos`,
      });
      
      setLocation("/orders");
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "No se pudo crear el pedido",
        variant: "destructive",
      });
    },
  });

  const canCreateOrder = () => {
    const hasProducts = scannedProducts.length > 0;
    const hasValidPrices = scannedProducts.every(item => item.salePrice && parseFloat(item.salePrice) > 0);
    const hasCustomer = selectedCustomerId || customerName.trim();
    const hasPaymentStatus = paymentStatus;
    const hasPaymentMethods = paymentMethods.length > 0;
    const hasDeliveryMethod = deliveryMethod;
    const hasValidAddress = deliveryMethod !== 'direccion' || deliveryAddress.trim();
    
    return hasProducts && hasValidPrices && hasCustomer && hasPaymentStatus && hasPaymentMethods && hasDeliveryMethod && hasValidAddress;
  };

  // Funciones para el arrastre del cuadro flotante
  const handleMouseDown = (e: React.MouseEvent) => {
    const rect = e.currentTarget.getBoundingClientRect();
    setIsDragging(true);
    setDragOffset({
      x: e.clientX - rect.left,
      y: e.clientY - rect.top
    });
  };

  const handleMouseMove = (e: MouseEvent) => {
    if (!isDragging) return;
    
    const newX = e.clientX - dragOffset.x;
    const newY = e.clientY - dragOffset.y;
    
    // Limitar el movimiento dentro de la ventana
    const maxX = window.innerWidth - 320; // 320px es el ancho del cuadro
    const maxY = window.innerHeight - 200; // Altura estimada del cuadro
    
    setPosition({
      x: Math.max(0, Math.min(newX, maxX)),
      y: Math.max(0, Math.min(newY, maxY))
    });
  };

  const handleMouseUp = () => {
    setIsDragging(false);
  };

  // Efectos para el arrastre
  useEffect(() => {
    if (isDragging) {
      document.addEventListener('mousemove', handleMouseMove);
      document.addEventListener('mouseup', handleMouseUp);
      document.body.style.cursor = 'grabbing';
      document.body.style.userSelect = 'none';
    } else {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    }

    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };
  }, [isDragging, dragOffset]);

  // Auto-agregar producto desactivado por solicitud del usuario
  // useEffect(() => {
  //   const addDefaultProduct = async () => {
  //     if (products && scannedProducts.length === 0) {
  //       const defaultIMEI = "356938035643811";
  //       const defaultProduct = products.find((p: Product) => p.imei === defaultIMEI);
  //       
  //       if (defaultProduct && defaultProduct.status === 'disponible') {
  //         const newOrderItem: OrderItem = {
  //           productId: defaultProduct.id,
  //           product: defaultProduct,
  //           salePrice: defaultPrice || "",
  //           imei: defaultProduct.imei,
  //           model: defaultProduct.model,
  //           color: defaultProduct.color,
  //           storage: defaultProduct.storage,
  //           status: defaultProduct.status,
  //           costPrice: defaultProduct.costPrice,
  //           battery: defaultProduct.battery,
  //           quality: defaultProduct.quality,
  //         };

  //         setScannedProducts([newOrderItem]);
  //         
  //         toast({
  //           title: "Producto agregado automáticamente",
  //           description: `${defaultProduct.model} ${defaultProduct.storage} agregado por defecto`,
  //         });
  //       }
  //     }
  //   };

  //   addDefaultProduct();
  // }, [products, defaultPrice, toast]);

  if (!isAuthenticated) {
    return null;
  }

  return (
    <div className="min-h-screen bg-white dark:bg-gray-900 flex flex-col">
      <Header onMobileMenuToggle={() => setMobileNavOpen(true)} />
      <MobileNav open={mobileNavOpen} onOpenChange={setMobileNavOpen} />
      
      <div className="flex flex-1">
        <Sidebar />
        
        <main className="flex-1 bg-white dark:bg-gray-900">
          <div className="max-w-7xl mx-auto px-6 py-6">
            
            {/* Header */}
            <div className="flex items-center mb-8">
              <ShoppingCart className="h-8 w-8 text-gray-600 dark:text-gray-400 mr-3" />
              <h1 className="text-2xl font-normal text-gray-800 dark:text-gray-200">
                Nuevo Pedido
              </h1>
            </div>

            {/* Información del cliente y vendedor - Movido arriba */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
              
              {/* Cliente Final */}
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  Cliente Final
                </label>
                <Select value={selectedCustomerId} onValueChange={(value) => setSelectedCustomerId(value)}>
                  <SelectTrigger className="h-10">
                    <SelectValue placeholder="Seleccionar..." />
                  </SelectTrigger>
                  <SelectContent>
                    {customers && Array.isArray(customers) && customers.map((customer: Customer) => (
                      <SelectItem key={customer.id} value={customer.id.toString()}>
                        {customer.name} {customer.dni ? `- DNI: ${customer.dni}` : ''}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {/* Vendedor */}
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  Vendedor
                </label>
                <Select value={selectedVendorId} onValueChange={(value) => setSelectedVendorId(value)}>
                  <SelectTrigger className="h-10">
                    <SelectValue placeholder="Seleccionar..." />
                  </SelectTrigger>
                  <SelectContent>
                    {vendors && Array.isArray(vendors) && vendors.map((vendor: any) => (
                      <SelectItem key={vendor.id} value={vendor.id.toString()}>
                        {vendor.name} {vendor.commission ? `(${vendor.commission}%)` : ''}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {/* Estado de pago */}
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  Estado de pago
                </label>
                <Select value={paymentStatus} onValueChange={(value) => setPaymentStatus(value as "" | "pagado" | "parcial" | "no_pagado")}>
                  <SelectTrigger className="h-10">
                    <SelectValue placeholder="Seleccionar..." />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="no_pagado">Pendiente</SelectItem>
                    <SelectItem value="pagado">Pagado</SelectItem>
                    <SelectItem value="parcial">Parcial</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>

            {/* Método de entrega */}
            <div className="mb-8">
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-4">
                Forma de Entrega
              </label>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {/* Opción Retiro en Oficina */}
                <div className={`flex items-center space-x-3 p-4 border rounded-lg cursor-pointer transition-all ${
                  deliveryMethod === "oficina" 
                    ? "border-blue-500 bg-blue-50 dark:bg-blue-900/20 dark:border-blue-400" 
                    : "border-gray-200 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700"
                }`} 
                     onClick={() => setDeliveryMethod("oficina")}>
                  <input
                    type="checkbox"
                    checked={deliveryMethod === "oficina"}
                    onChange={() => setDeliveryMethod("oficina")}
                    className="w-4 h-4 text-blue-600 bg-gray-100 border-gray-300 rounded focus:ring-blue-500 dark:focus:ring-blue-600 dark:ring-offset-gray-800 focus:ring-2 dark:bg-gray-700 dark:border-gray-600"
                  />
                  <div>
                    <label className="text-sm font-medium text-gray-900 dark:text-white cursor-pointer">
                      Retiro en Oficina
                    </label>
                    <p className="text-xs text-gray-500 dark:text-gray-400">
                      El cliente retira el producto en nuestras oficinas
                    </p>
                  </div>
                </div>

                {/* Opción Dirección de Entrega */}
                <div className={`flex items-center space-x-3 p-4 border rounded-lg cursor-pointer transition-all ${
                  deliveryMethod === "direccion" 
                    ? "border-green-500 bg-green-50 dark:bg-green-900/20 dark:border-green-400" 
                    : "border-gray-200 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700"
                }`}
                     onClick={() => setDeliveryMethod("direccion")}>
                  <input
                    type="checkbox"
                    checked={deliveryMethod === "direccion"}
                    onChange={() => setDeliveryMethod("direccion")}
                    className="w-4 h-4 text-green-600 bg-gray-100 border-gray-300 rounded focus:ring-green-500 dark:focus:ring-green-600 dark:ring-offset-gray-800 focus:ring-2 dark:bg-gray-700 dark:border-gray-600"
                  />
                  <div>
                    <label className="text-sm font-medium text-gray-900 dark:text-white cursor-pointer">
                      Dirección de Entrega
                    </label>
                    <p className="text-xs text-gray-500 dark:text-gray-400">
                      Envío a domicilio del cliente
                    </p>
                  </div>
                </div>
              </div>

              {/* Campo de dirección - Solo visible cuando se selecciona "Dirección de Entrega" */}
              {deliveryMethod === "direccion" && (
                <div className="mt-4">
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                    Dirección de Entrega
                  </label>
                  <Input
                    type="text"
                    value={deliveryAddress}
                    onChange={(e) => setDeliveryAddress(e.target.value)}
                    placeholder="Ingrese la dirección completa (calle, número, ciudad, provincia)"
                    className="h-10 w-full"
                  />
                </div>
              )}
            </div>





            {/* Escáner IMEI */}
            <div className="mb-8">
              <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-4">
                Escáner de IMEI
              </h3>
              
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <div>
                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Escanear IMEI</Label>
                  <Input
                    ref={imeiInputRef}
                    type="text"
                    value={imeiInput}
                    onChange={(e) => setImeiInput(e.target.value)}
                    onKeyDown={handleKeyPress}
                    placeholder="Escanear o ingresar IMEI"
                    className="h-12 text-lg font-mono"
                  />
                </div>
                <div>
                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Precio de Venta</Label>
                  <Input
                    type="number"
                    step="0.01"
                    value={defaultPrice}
                    onChange={(e) => setDefaultPrice(e.target.value)}
                    placeholder="0.00"
                    className="h-12 text-lg"
                  />
                </div>
              </div>
              
              <div className="mt-4 flex flex-wrap gap-3">
                <Button
                  onClick={handleScanIMEI}
                  disabled={!imeiInput.trim()}
                  className="bg-blue-600 hover:bg-blue-700 text-white"
                >
                  Buscar Producto
                </Button>
                
                <Button
                  variant="outline"
                  onClick={clearScannedProducts}
                  disabled={scannedProducts.length === 0}
                  className="border-red-300 hover:bg-red-50 text-red-600 dark:border-red-600 dark:hover:bg-red-900 dark:text-red-400"
                >
                  Limpiar Lista
                </Button>
              </div>
            </div>

            {/* Productos agregados - Debajo del escaner */}
            <div className="mb-8">
              <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-4">
                Resumen de Productos:
              </h3>
              
              {scannedProducts.length === 0 ? (
                <div className="text-center py-8 text-gray-500 dark:text-gray-400 border border-gray-300 dark:border-gray-600 rounded-md">
                  No hay productos agregados
                </div>
              ) : (
                <div className="border border-gray-300 dark:border-gray-600 rounded-md overflow-hidden">
                  <div className="overflow-x-auto">
                    <table className="w-full border-collapse">
                      <thead>
                        <tr className="bg-gray-50 dark:bg-gray-800">
                          <th className="border-b border-gray-300 dark:border-gray-600 px-4 py-3 text-left text-sm font-medium text-gray-700 dark:text-gray-300">IMEI</th>
                          <th className="border-b border-gray-300 dark:border-gray-600 px-4 py-3 text-left text-sm font-medium text-gray-700 dark:text-gray-300">Modelo</th>
                          <th className="border-b border-gray-300 dark:border-gray-600 px-4 py-3 text-left text-sm font-medium text-gray-700 dark:text-gray-300">Almacenamiento</th>
                          <th className="border-b border-gray-300 dark:border-gray-600 px-4 py-3 text-left text-sm font-medium text-gray-700 dark:text-gray-300">Estado</th>
                          <th className="border-b border-gray-300 dark:border-gray-600 px-4 py-3 text-left text-sm font-medium text-gray-700 dark:text-gray-300">Color</th>
                          <th className="border-b border-gray-300 dark:border-gray-600 px-4 py-3 text-left text-sm font-medium text-gray-700 dark:text-gray-300">Precio de Venta</th>
                          <th className="border-b border-gray-300 dark:border-gray-600 px-4 py-3 text-left text-sm font-medium text-gray-700 dark:text-gray-300">Eliminar</th>
                        </tr>
                      </thead>
                      <tbody className="bg-white dark:bg-gray-900">
                        {scannedProducts.map((item, index) => (
                          <tr key={index} className="hover:bg-gray-50 dark:hover:bg-gray-800 border-b border-gray-200 dark:border-gray-700 last:border-b-0">
                            <td className="px-4 py-3 text-sm text-gray-900 dark:text-gray-100">
                              {item.imei}
                            </td>
                            <td className="px-4 py-3 text-sm font-medium text-gray-900 dark:text-gray-100">
                              {item.model}
                            </td>
                            <td className="px-4 py-3 text-sm text-gray-900 dark:text-gray-100">
                              {item.storage || 'N/A'}
                            </td>
                            <td className="px-4 py-3 text-sm">
                              <span className={`inline-flex px-2 py-1 text-xs font-medium rounded-full ${
                                item.status === 'disponible' 
                                  ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
                                  : item.status === 'reservado'
                                  ? 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200'
                                  : 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
                              }`}>
                                {item.status?.charAt(0).toUpperCase() + item.status?.slice(1) || 'N/A'}
                              </span>
                            </td>
                            <td className="px-4 py-3 text-sm text-gray-900 dark:text-gray-100">
                              {item.color}
                            </td>
                            <td className="px-4 py-3">
                              <Input
                                type="number"
                                step="0.01"
                                placeholder="0.00"
                                value={item.salePrice}
                                onChange={(e) => handlePriceChange(index, e.target.value)}
                                onKeyDown={(e) => handlePriceKeyPress(e, index)}
                                data-price-index={index}
                                className="h-9 text-sm w-28 border-gray-300 dark:border-gray-600 focus:border-blue-500 dark:focus:border-blue-400"
                              />
                            </td>
                            <td className="px-4 py-3">
                              <Button
                                variant="outline"
                                size="sm"
                                onClick={() => removeProduct(item.productId)}
                                className="h-8 w-8 p-0 border-red-300 hover:bg-red-50 hover:border-red-400 dark:border-red-600 dark:hover:bg-red-900"
                              >
                                <Trash2 className="h-4 w-4 text-red-500" />
                              </Button>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>

                  {/* Total del pedido */}
                  <div className="bg-gray-50 dark:bg-gray-800 px-4 py-3 border-t border-gray-300 dark:border-gray-600">
                    <div className="flex justify-between items-center">
                      <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
                        Total de productos: {scannedProducts.length}
                      </span>
                      <span className="text-lg font-bold text-gray-900 dark:text-gray-100">
                        Total: ${calculateTotal().toFixed(2)}
                      </span>
                    </div>
                  </div>
                </div>
              )}
            </div>

            {/* Mostrar métodos de pago si hay productos o si se ha configurado cliente/vendedor */}
            {(scannedProducts.length > 0 || selectedCustomerId || selectedVendorId) && (
              <div className="mb-8">
                <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-4">
                  Métodos de Pago:
                </h3>
                
                <div className="bg-gray-50 dark:bg-gray-800 p-6 rounded-lg border border-gray-200 dark:border-gray-700">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    {availablePaymentMethods.map((method) => (
                      <div key={method.value} className="border border-gray-300 dark:border-gray-600 rounded-lg p-4 bg-white dark:bg-gray-900">
                        <div className="flex items-center space-x-3 mb-3">
                          <input
                            type="checkbox"
                            id={`payment-${method.value}`}
                            checked={paymentMethods.includes(method.value)}
                            onChange={(e) => {
                              if (e.target.checked) {
                                setPaymentMethods(prev => [...prev, method.value]);
                                setPaymentAmounts(prev => ({ ...prev, [method.value]: '' }));
                              } else {
                                setPaymentMethods(prev => prev.filter(m => m !== method.value));
                                setPaymentAmounts(prev => {
                                  const newAmounts = { ...prev };
                                  delete newAmounts[method.value];
                                  return newAmounts;
                                });
                                if (method.value === 'financiera_usd') {
                                  setFinancieraUSDAmount('');
                                }
                                if (method.value === 'financiera_ars') {
                                  setFinancieraARSAmount('');
                                }
                              }
                            }}
                            className="rounded h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 dark:border-gray-600"
                          />
                          <label htmlFor={`payment-${method.value}`} className="font-medium text-gray-900 dark:text-gray-100 cursor-pointer">
                            {method.label}
                          </label>
                        </div>
                        
                        {/* Campo de monto para todos los métodos de pago */}
                        {paymentMethods.includes(method.value) && (
                          <div className="mt-3 space-y-3">
                            {method.value === 'financiera_usd' ? (
                              // Campos especiales para financiera USD→ARS
                              <div className="space-y-3">
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Monto USD</Label>
                                  <Input
                                    type="number"
                                    value={financieraUSDAmount}
                                    onChange={(e) => setFinancieraUSDAmount(e.target.value)}
                                    placeholder="0.00"
                                    min="0"
                                    step="0.01"
                                    className="h-9 w-full"
                                  />
                                </div>
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Equivalente ARS</Label>
                                  <Input
                                    type="text"
                                    value={`$${calculateARSFromUSDFinanciera(financieraUSDAmount)}`}
                                    readOnly
                                    className="h-9 w-full bg-gray-50 dark:bg-gray-800 text-gray-700 dark:text-gray-300"
                                  />
                                </div>
                                <div className="flex items-center justify-between">
                                  <span className="text-xs text-gray-500 dark:text-gray-400">
                                    TC: ${Math.round(parseFloat(paymentExchangeRates['financiera_usd_ars']) || 1100)}
                                  </span>
                                  <Button
                                    type="button"
                                    variant="outline"
                                    size="sm"
                                    onClick={() => {
                                      const newRate = prompt('Configurar tasa de conversión FINANCIERA USD→ARS:', paymentExchangeRates['financiera_usd_ars']);
                                      if (newRate && !isNaN(parseFloat(newRate)) && parseFloat(newRate) > 0) {
                                        updatePaymentExchangeRate('financiera_usd_ars', newRate);
                                        toast({
                                          title: "Tasa Financiera USD actualizada",
                                          description: `Configurada en ${Math.round(parseFloat(newRate))} ARS/USD`,
                                        });
                                      } else if (newRate !== null) {
                                        toast({
                                          title: "Error",
                                          description: "Ingrese una tasa válida mayor a 0",
                                          variant: "destructive"
                                        });
                                      }
                                    }}
                                    className="h-7 px-3 text-xs"
                                  >
                                    Configurar TC
                                  </Button>
                                </div>
                              </div>
                            ) : method.value === 'financiera_ars' ? (
                              // Campos especiales para financiera ARS→USD
                              <div className="space-y-3">
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Monto ARS</Label>
                                  <Input
                                    type="number"
                                    value={financieraARSAmount}
                                    onChange={(e) => setFinancieraARSAmount(e.target.value)}
                                    placeholder="0.00"
                                    min="0"
                                    step="0.01"
                                    className="h-9 w-full"
                                  />
                                </div>
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Equivalente USD</Label>
                                  <Input
                                    type="text"
                                    value={`$${calculateUSDFromARSFinanciera(financieraARSAmount)}`}
                                    readOnly
                                    className="h-9 w-full bg-gray-50 dark:bg-gray-800 text-gray-700 dark:text-gray-300"
                                  />
                                </div>
                                <div className="flex items-center justify-between">
                                  <span className="text-xs text-gray-500 dark:text-gray-400">
                                    TC: ${Math.round(parseFloat(paymentExchangeRates['financiera_ars_usd']) || 1050)}
                                  </span>
                                  <Button
                                    type="button"
                                    variant="outline"
                                    size="sm"
                                    onClick={() => {
                                      const newRate = prompt('Configurar tasa de conversión FINANCIERA ARS→USD:', paymentExchangeRates['financiera_ars_usd']);
                                      if (newRate && !isNaN(parseFloat(newRate)) && parseFloat(newRate) > 0) {
                                        updatePaymentExchangeRate('financiera_ars_usd', newRate);
                                        toast({
                                          title: "Tasa Financiera ARS actualizada",
                                          description: `Configurada en ${Math.round(parseFloat(newRate))} ARS/USD`,
                                        });
                                      } else if (newRate !== null) {
                                        toast({
                                          title: "Error",
                                          description: "Ingrese una tasa válida mayor a 0",
                                          variant: "destructive"
                                        });
                                      }
                                    }}
                                    className="h-7 px-3 text-xs"
                                  >
                                    Configurar TC
                                  </Button>
                                </div>
                              </div>
                            ) : method.value === 'efectivo_usd' ? (
                              // Efectivo USD con conversión a ARS
                              <div className="space-y-3">
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Monto USD</Label>
                                  <Input
                                    type="number"
                                    value={paymentAmounts[method.value] || ''}
                                    onChange={(e) => setPaymentAmounts(prev => ({
                                      ...prev,
                                      [method.value]: e.target.value
                                    }))}
                                    placeholder="0.00"
                                    min="0"
                                    step="0.01"
                                    className="h-9 w-full"
                                  />
                                </div>
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Equivalente ARS</Label>
                                  <Input
                                    type="text"
                                    value={`$${((parseFloat(paymentAmounts[method.value] || '0') * (parseFloat(paymentExchangeRates['efectivo_usd']) || 1100))).toFixed(2)}`}
                                    readOnly
                                    className="h-9 w-full bg-gray-50 dark:bg-gray-800 text-gray-700 dark:text-gray-300"
                                  />
                                </div>
                                <div className="flex items-center justify-between">
                                  <span className="text-xs text-gray-500 dark:text-gray-400">
                                    TC: ${Math.round(parseFloat(paymentExchangeRates['efectivo_usd']) || 1100)}
                                  </span>
                                  <Button
                                    type="button"
                                    variant="outline"
                                    size="sm"
                                    onClick={() => {
                                      const newRate = prompt('Configurar tasa Efectivo USD→ARS:', paymentExchangeRates['efectivo_usd']);
                                      if (newRate && !isNaN(parseFloat(newRate)) && parseFloat(newRate) > 0) {
                                        updatePaymentExchangeRate('efectivo_usd', newRate);
                                        toast({
                                          title: "Tasa Efectivo USD actualizada",
                                          description: `Configurada en ${Math.round(parseFloat(newRate))} ARS/USD`,
                                        });
                                      }
                                    }}
                                    className="h-7 px-3 text-xs"
                                  >
                                    Configurar TC
                                  </Button>
                                </div>
                              </div>
                            ) : method.value === 'efectivo_ars' ? (
                              // Efectivo ARS con conversión a USD
                              <div className="space-y-3">
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Monto ARS</Label>
                                  <Input
                                    type="number"
                                    value={paymentAmounts[method.value] || ''}
                                    onChange={(e) => setPaymentAmounts(prev => ({
                                      ...prev,
                                      [method.value]: e.target.value
                                    }))}
                                    placeholder="0.00"
                                    min="0"
                                    step="0.01"
                                    className="h-9 w-full"
                                  />
                                </div>
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Equivalente USD</Label>
                                  <Input
                                    type="text"
                                    value={`$${((parseFloat(paymentAmounts[method.value] || '0') / (parseFloat(paymentExchangeRates['efectivo_ars']) || 1100))).toFixed(2)}`}
                                    readOnly
                                    className="h-9 w-full bg-gray-50 dark:bg-gray-800 text-gray-700 dark:text-gray-300"
                                  />
                                </div>
                                <div className="flex items-center justify-between">
                                  <span className="text-xs text-gray-500 dark:text-gray-400">
                                    TC: ${Math.round(parseFloat(paymentExchangeRates['efectivo_ars']) || 1100)}
                                  </span>
                                  <Button
                                    type="button"
                                    variant="outline"
                                    size="sm"
                                    onClick={() => {
                                      const newRate = prompt('Configurar tasa Efectivo ARS→USD:', paymentExchangeRates['efectivo_ars']);
                                      if (newRate && !isNaN(parseFloat(newRate)) && parseFloat(newRate) > 0) {
                                        updatePaymentExchangeRate('efectivo_ars', newRate);
                                        toast({
                                          title: "Tasa Efectivo ARS actualizada",
                                          description: `Configurada en ${Math.round(parseFloat(newRate))} ARS/USD`,
                                        });
                                      }
                                    }}
                                    className="h-7 px-3 text-xs"
                                  >
                                    Configurar TC
                                  </Button>
                                </div>
                              </div>
                            ) : method.value === 'transferencia_usd' ? (
                              // Transferencia USD con conversión a ARS
                              <div className="space-y-3">
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Monto USD</Label>
                                  <Input
                                    type="number"
                                    value={paymentAmounts[method.value] || ''}
                                    onChange={(e) => setPaymentAmounts(prev => ({
                                      ...prev,
                                      [method.value]: e.target.value
                                    }))}
                                    placeholder="0.00"
                                    min="0"
                                    step="0.01"
                                    className="h-9 w-full"
                                  />
                                </div>
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Equivalente ARS</Label>
                                  <Input
                                    type="text"
                                    value={`$${((parseFloat(paymentAmounts[method.value] || '0') * (parseFloat(paymentExchangeRates['transferencia_usd']) || 1080))).toFixed(2)}`}
                                    readOnly
                                    className="h-9 w-full bg-gray-50 dark:bg-gray-800 text-gray-700 dark:text-gray-300"
                                  />
                                </div>
                                <div className="flex items-center justify-between">
                                  <span className="text-xs text-gray-500 dark:text-gray-400">
                                    TC: ${Math.round(parseFloat(paymentExchangeRates['transferencia_usd']) || 1080)}
                                  </span>
                                  <Button
                                    type="button"
                                    variant="outline"
                                    size="sm"
                                    onClick={() => {
                                      const newRate = prompt('Configurar tasa Transferencia USD→ARS:', paymentExchangeRates['transferencia_usd']);
                                      if (newRate && !isNaN(parseFloat(newRate)) && parseFloat(newRate) > 0) {
                                        updatePaymentExchangeRate('transferencia_usd', newRate);
                                        toast({
                                          title: "Tasa Transferencia USD actualizada",
                                          description: `Configurada en ${Math.round(parseFloat(newRate))} ARS/USD`,
                                        });
                                      }
                                    }}
                                    className="h-7 px-3 text-xs"
                                  >
                                    Configurar TC
                                  </Button>
                                </div>
                              </div>
                            ) : method.value === 'transferencia_ars' ? (
                              // Transferencia ARS con conversión a USD
                              <div className="space-y-3">
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Monto ARS</Label>
                                  <Input
                                    type="number"
                                    value={paymentAmounts[method.value] || ''}
                                    onChange={(e) => setPaymentAmounts(prev => ({
                                      ...prev,
                                      [method.value]: e.target.value
                                    }))}
                                    placeholder="0.00"
                                    min="0"
                                    step="0.01"
                                    className="h-9 w-full"
                                  />
                                </div>
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Equivalente USD</Label>
                                  <Input
                                    type="text"
                                    value={`$${((parseFloat(paymentAmounts[method.value] || '0') / (parseFloat(paymentExchangeRates['transferencia_ars']) || 1080))).toFixed(2)}`}
                                    readOnly
                                    className="h-9 w-full bg-gray-50 dark:bg-gray-800 text-gray-700 dark:text-gray-300"
                                  />
                                </div>
                                <div className="flex items-center justify-between">
                                  <span className="text-xs text-gray-500 dark:text-gray-400">
                                    TC: ${Math.round(parseFloat(paymentExchangeRates['transferencia_ars']) || 1080)}
                                  </span>
                                  <Button
                                    type="button"
                                    variant="outline"
                                    size="sm"
                                    onClick={() => {
                                      const newRate = prompt('Configurar tasa Transferencia ARS→USD:', paymentExchangeRates['transferencia_ars']);
                                      if (newRate && !isNaN(parseFloat(newRate)) && parseFloat(newRate) > 0) {
                                        updatePaymentExchangeRate('transferencia_ars', newRate);
                                        toast({
                                          title: "Tasa Transferencia ARS actualizada",
                                          description: `Configurada en ${Math.round(parseFloat(newRate))} ARS/USD`,
                                        });
                                      }
                                    }}
                                    className="h-7 px-3 text-xs"
                                  >
                                    Configurar TC
                                  </Button>
                                </div>
                              </div>
                            ) : method.value === 'transferencia_usdt' ? (
                              // Transferencia USDT con conversión a ARS
                              <div className="space-y-3">
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Monto USDT</Label>
                                  <Input
                                    type="number"
                                    value={paymentAmounts[method.value] || ''}
                                    onChange={(e) => setPaymentAmounts(prev => ({
                                      ...prev,
                                      [method.value]: e.target.value
                                    }))}
                                    placeholder="0.00"
                                    min="0"
                                    step="0.01"
                                    className="h-9 w-full"
                                  />
                                </div>
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">Equivalente ARS</Label>
                                  <Input
                                    type="text"
                                    value={`$${((parseFloat(paymentAmounts[method.value] || '0') * (parseFloat(paymentExchangeRates['transferencia_usdt']) || 1070))).toFixed(2)}`}
                                    readOnly
                                    className="h-9 w-full bg-gray-50 dark:bg-gray-800 text-gray-700 dark:text-gray-300"
                                  />
                                </div>
                                <div className="flex items-center justify-between">
                                  <span className="text-xs text-gray-500 dark:text-gray-400">
                                    TC: ${Math.round(parseFloat(paymentExchangeRates['transferencia_usdt']) || 1070)}
                                  </span>
                                  <Button
                                    type="button"
                                    variant="outline"
                                    size="sm"
                                    onClick={() => {
                                      const newRate = prompt('Configurar tasa Transferencia USDT→ARS:', paymentExchangeRates['transferencia_usdt']);
                                      if (newRate && !isNaN(parseFloat(newRate)) && parseFloat(newRate) > 0) {
                                        updatePaymentExchangeRate('transferencia_usdt', newRate);
                                        toast({
                                          title: "Tasa Transferencia USDT actualizada",
                                          description: `Configurada en ${Math.round(parseFloat(newRate))} ARS/USD`,
                                        });
                                      }
                                    }}
                                    className="h-7 px-3 text-xs"
                                  >
                                    Configurar TC
                                  </Button>
                                </div>
                              </div>
                            ) : (
                              // Campo normal para otros métodos
                              <div className="space-y-3">
                                <div>
                                  <Label className="text-sm text-gray-600 dark:text-gray-400 mb-1">
                                    Monto ({method.currency})
                                  </Label>
                                  <Input
                                    type="number"
                                    value={paymentAmounts[method.value] || ''}
                                    onChange={(e) => setPaymentAmounts(prev => ({
                                      ...prev,
                                      [method.value]: e.target.value
                                    }))}
                                    placeholder="0.00"
                                    min="0"
                                    step="0.01"
                                    className="h-9 w-full"
                                  />
                                </div>
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                  

                  
                  {/* Campo de Observaciones */}
                  <div className="mt-6">
                    <Label className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2 block">
                      Observaciones (Opcional)
                    </Label>
                    <textarea
                      rows={3}
                      className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-900 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400 focus:border-blue-500 dark:focus:border-blue-400 focus:ring-1 focus:ring-blue-500 dark:focus:ring-blue-400 resize-none"
                      placeholder="Agregar observaciones sobre el pedido o los métodos de pago..."
                      value={orderNotes}
                      onChange={(e) => setOrderNotes(e.target.value)}
                    />
                  </div>
                </div>
              </div>
            )}

            {/* Botón Guardar - Solo visible si hay productos y métodos de pago configurados */}
            {scannedProducts.length > 0 && paymentMethods.length > 0 && selectedCustomerId && (
              <div className="flex justify-start">
                <Button
                  onClick={() => createOrderMutation.mutate()}
                  disabled={!canCreateOrder() || createOrderMutation.isPending}
                  className="bg-blue-600 hover:bg-blue-700 text-white px-8 py-2 text-lg h-12"
                >
                  {createOrderMutation.isPending ? "Guardando..." : "Guardar Pedido"}
                </Button>
              </div>
            )}
          </div>
        </main>
        
        {/* Cuadrito flotante de resumen de pagos - arrastrable */}
        {(paymentMethods.length > 0 || scannedProducts.length > 0) && (
          <div 
            className="fixed w-80 bg-white dark:bg-gray-800 shadow-2xl border border-gray-200 dark:border-gray-700 rounded-lg p-4 z-50 transition-transform"
            style={{
              top: position.y || 80,
              right: position.x ? `${window.innerWidth - position.x - 320}px` : 24,
              left: position.x ? `${position.x}px` : 'auto',
              cursor: isDragging ? 'grabbing' : 'grab',
              transform: isDragging ? 'scale(1.02)' : 'scale(1)'
            }}
            onMouseDown={handleMouseDown}
          >
            <div className="flex items-center justify-between mb-3">
              <h4 className="text-sm font-semibold text-gray-900 dark:text-gray-100 flex items-center gap-2">
                <span className="cursor-move">🤏</span>
                💰 Resumen de Pago
              </h4>
              <div className="h-2 w-2 bg-blue-500 rounded-full animate-pulse"></div>
            </div>
            
            {/* Productos escaneados */}
            {scannedProducts.length > 0 && (
              <div className="mb-3">
                <div className="flex justify-between items-center text-sm">
                  <span className="text-gray-600 dark:text-gray-400">Productos ({scannedProducts.length}):</span>
                  <span className="font-medium text-gray-900 dark:text-gray-100">
                    USD {scannedProducts.reduce((sum, product) => {
                      const salePrice = parseFloat(product.salePrice || '0');
                      return sum + salePrice;
                    }, 0).toFixed(2)}
                  </span>
                </div>
              </div>
            )}
            
            {/* Métodos de pago */}
            {paymentMethods.length > 0 && (
              <div className="space-y-1 mb-3">
                {paymentMethods.map((methodValue) => {
                  const method = availablePaymentMethods.find(m => m.value === methodValue);
                  let amount = '0.00';
                  
                  if (methodValue === 'financiera_usd') {
                    amount = financieraUSDAmount || '0.00';
                  } else if (methodValue === 'financiera_ars') {
                    amount = financieraARSAmount || '0.00';
                  } else {
                    amount = paymentAmounts[methodValue] || '0.00';
                  }
                  
                  return (
                    <div key={methodValue} className="flex justify-between items-center text-xs">
                      <span className="text-gray-600 dark:text-gray-400 truncate">{method?.label}:</span>
                      <span className="font-medium text-gray-900 dark:text-gray-100 ml-2">
                        {method?.currency} {amount}
                      </span>
                    </div>
                  );
                })}
              </div>
            )}
            
            {/* Totales y balance */}
            <div className="border-t border-gray-200 dark:border-gray-700 pt-3 space-y-2">
              <div className="flex justify-between items-center text-sm">
                <span className="font-medium text-gray-700 dark:text-gray-300">Total Pagado:</span>
                <span className="font-semibold text-blue-600 dark:text-blue-400">
                  USD {(() => {
                    let total = 0;
                    paymentMethods.forEach(methodValue => {
                      const method = availablePaymentMethods.find(m => m.value === methodValue);
                      let amount = 0;
                      
                      if (methodValue === 'financiera_usd') {
                        amount = parseFloat(financieraUSDAmount || '0');
                      } else if (methodValue === 'financiera_ars') {
                        const arsAmount = parseFloat(financieraARSAmount || '0');
                        const rate = parseFloat(paymentExchangeRates['financiera_ars_usd'] || '1050');
                        amount = arsAmount / rate;
                      } else {
                        const methodAmount = parseFloat(paymentAmounts[methodValue] || '0');
                        if (method?.currency === 'ARS') {
                          // Usar tipo de cambio específico del método
                          const rate = parseFloat(paymentExchangeRates[methodValue] || '1000');
                          amount = methodAmount / rate;
                        } else {
                          amount = methodAmount;
                        }
                      }
                      total += amount;
                    });
                    return total.toFixed(2);
                  })()}
                </span>
              </div>
              
              {/* Balance restante */}
              {scannedProducts.length > 0 && (() => {
                const totalProductsUSD = scannedProducts.reduce((sum, product) => {
                  const salePrice = parseFloat(product.salePrice || '0');
                  return sum + salePrice;
                }, 0);
                
                let totalPaidUSD = 0;
                paymentMethods.forEach(methodValue => {
                  const method = availablePaymentMethods.find(m => m.value === methodValue);
                  let amount = 0;
                  
                  if (methodValue === 'financiera_usd') {
                    amount = parseFloat(financieraUSDAmount || '0');
                  } else if (methodValue === 'financiera_ars') {
                    const arsAmount = parseFloat(financieraARSAmount || '0');
                    const rate = parseFloat(paymentExchangeRates['financiera_ars_usd'] || '1050');
                    amount = arsAmount / rate;
                  } else {
                    const methodAmount = parseFloat(paymentAmounts[methodValue] || '0');
                    if (method?.currency === 'ARS') {
                      // Usar tipo de cambio específico del método
                      const rate = parseFloat(paymentExchangeRates[methodValue] || '1000');
                      amount = methodAmount / rate;
                    } else {
                      amount = methodAmount;
                    }
                  }
                  totalPaidUSD += amount;
                });
                
                const remainingUSD = totalProductsUSD - totalPaidUSD;
                
                return (
                  <div className={`flex justify-between items-center text-sm font-semibold border-t border-gray-200 dark:border-gray-700 pt-2 ${remainingUSD > 0 ? 'text-red-600 dark:text-red-400' : remainingUSD < 0 ? 'text-orange-600 dark:text-orange-400' : 'text-green-600 dark:text-green-400'}`}>
                    <span>
                      {remainingUSD > 0 ? '⚠️ Falta:' : remainingUSD < 0 ? '💰 Exceso:' : '✅ Pagado:'}
                    </span>
                    <span className="text-lg">
                      USD {Math.abs(remainingUSD).toFixed(2)}
                    </span>
                  </div>
                );
              })()}
            </div>
          </div>
        )}
      </div>
      <Footer />
    </div>
  );
}
