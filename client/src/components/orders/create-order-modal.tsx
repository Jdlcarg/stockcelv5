import { useState, useEffect } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useAuth } from "@/lib/auth";
import { apiRequest } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Trash2, Plus, ShoppingCart, Search, Calculator } from "lucide-react";
import CurrencySwap from "@/components/currency/currency-swap";

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
  paymentMethod: string;
  exchangeRate?: string;
  amountUsd?: string;
}

interface CreateOrderModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export default function CreateOrderModal({ open, onOpenChange }: CreateOrderModalProps) {
  const { user } = useAuth();
  const { toast } = useToast();
  const queryClient = useQueryClient();
  
  const [orderData, setOrderData] = useState({
    customerName: "",
    customerPhone: "",
    customerEmail: "",
    customerId: "",
    vendorId: "",
  });
  
  const [selectedItems, setSelectedItems] = useState<OrderItem[]>([]);
  const [scannedProducts, setScannedProducts] = useState<Product[]>([]);
  const [selectedProductId, setSelectedProductId] = useState<string>("");
  const [salePrice, setSalePrice] = useState("");
  const [paymentMethod, setPaymentMethod] = useState("");
  const [imeiInput, setImeiInput] = useState("");
  const [scanningMode, setScanningMode] = useState<'scanning' | 'pricing'>('scanning');
  const [showSwap, setShowSwap] = useState(false);
  const [currentItemIndex, setCurrentItemIndex] = useState<number>(-1);
  const [paymentStatus, setPaymentStatus] = useState<"total" | "parcial" | "no_pagado" | "">("");
  const [orderPaymentMethods, setOrderPaymentMethods] = useState<string[]>([]);
  const [productPrices, setProductPrices] = useState<{[key: number]: string}>({});
  const [paymentAmounts, setPaymentAmounts] = useState<{[key: string]: string}>({});
  const [observations, setObservations] = useState("");
  const [shippingType, setShippingType] = useState<"oficina" | "direccion" | "">("");
  const [shippingAddress, setShippingAddress] = useState("");
  const [exchangeRates, setExchangeRates] = useState({
    efectivo_pesos: 1000,
    transferencia_pesos: 1000,
    transferencia_financiera: 1050,
    financiera: 1100,
  });

  // Obtener productos disponibles
  const { data: products } = useQuery({
    queryKey: ['/api/products', user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/products?clientId=${user?.clientId}`);
      return response.json();
    },
    enabled: !!user?.clientId && open,
  });

  // Obtener clientes disponibles
  const { data: customers } = useQuery({
    queryKey: ['/api/customers', user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/customers?clientId=${user?.clientId}`);
      return response.json();
    },
    enabled: !!user?.clientId && open,
  });

  // Obtener vendedores disponibles
  const { data: vendors } = useQuery({
    queryKey: ['/api/vendors', user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/vendors?clientId=${user?.clientId}`);
      return response.json();
    },
    enabled: !!user?.clientId && open,
  });

  // Filtrar productos disponibles que no estén ya en el pedido
  const availableProducts = products?.filter((product: Product) => 
    product.status === 'disponible' && 
    !selectedItems.some(item => item.productId === product.id)
  ) || [];

  const createOrderMutation = useMutation({
    mutationFn: async (data: any) => {
      // Crear el pedido
      const orderNumber = `ORD-${Date.now()}`;
      const totalUsd = selectedItems.reduce((sum, item) => sum + parseFloat(item.salePrice), 0);
      
      const order = await apiRequest('POST', '/api/orders', {
        clientId: user?.clientId,
        orderNumber,
        customerName: data.customerName,
        customerPhone: data.customerPhone || null,
        customerEmail: data.customerEmail || null,
        customerId: data.customerId ? parseInt(data.customerId) : null,
        totalUsd: totalUsd.toFixed(2),
        status: 'pendiente',
        paymentStatus: data.paymentStatus || 'pendiente',
        observations: observations || null,
        shippingType: shippingType || null,
        shippingAddress: shippingType === 'direccion' ? shippingAddress || null : null,
        vendorId: data.vendorId && data.vendorId !== "none" ? parseInt(data.vendorId) : user?.id,
      });

      // Agregar items al pedido
      for (const item of selectedItems) {
        await apiRequest('POST', '/api/order-items', {
          orderId: order.id,
          productId: item.productId,
          quantity: 1,
          priceUsd: parseFloat(item.salePrice),
          paymentMethod: orderPaymentMethods.join(', '),
          amountUsd: parseFloat(item.salePrice),
        });

        // Actualizar el estado del producto a 'vendido'
        await apiRequest('PUT', `/api/products/${item.productId}`, {
          status: 'vendido',
          userId: user?.id,
        });
      }

      return order;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['/api/orders'] });
      queryClient.invalidateQueries({ queryKey: ['/api/products'] });
      
      toast({
        title: "Éxito",
        description: "Pedido creado correctamente",
      });
      
      setOrderData({ customerName: "", customerPhone: "", customerEmail: "", customerId: "", vendorId: "" });
      setSelectedItems([]);
      setScannedProducts([]);
      setSelectedProductId("");
      setSalePrice("");
      setPaymentMethod("");
      setPaymentStatus("");
      setOrderPaymentMethods([]);
      setProductPrices({});
      setPaymentAmounts({});
      setObservations("");
      setShippingType("");
      setShippingAddress("");
      setImeiInput("");
      setScanningMode('scanning');
      onOpenChange(false);
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "No se pudo crear el pedido",
        variant: "destructive",
      });
    },
  });

  const paymentMethods = [
    { value: "efectivo_dolar", label: "Efectivo Dólar" },
    { value: "efectivo_pesos", label: "Efectivo Pesos" },
    { value: "transferencia_pesos", label: "Transferencia Pesos" },
    { value: "transferencia_usdt", label: "Transferencia USDT" },
    { value: "transferencia_financiera", label: "Transferencia Financiera" },
    { value: "financiera", label: "Financiera" },
  ];

  const handleScanIMEI = async () => {
    if (!imeiInput.trim()) {
      toast({
        title: "Error",
        description: "Debes ingresar un IMEI",
        variant: "destructive",
      });
      return;
    }

    // Refresh products data first
    await queryClient.invalidateQueries({ queryKey: ['/api/products', user?.clientId] });
    
    // Wait for fresh data
    const freshProductsResponse = await fetch(`/api/products?clientId=${user?.clientId}`);
    const freshProducts = await freshProductsResponse.json();
    
    const product = freshProducts?.find((p: Product) => p.imei === imeiInput.trim());
    if (!product) {
      toast({
        title: "Error",
        description: "No se encontró un producto con ese IMEI",
        variant: "destructive",
      });
      return;
    }

    if (!['disponible', 'reservado'].includes(product.status)) {
      toast({
        title: "Error",
        description: "El producto no está disponible para venta",
        variant: "destructive",
      });
      return;
    }

    if (scannedProducts.some(p => p.id === product.id)) {
      toast({
        title: "Error",
        description: "Este producto ya fue escaneado",
        variant: "destructive",
      });
      return;
    }

    // En modo scanning, agregamos a la lista de productos escaneados
    if (scanningMode === 'scanning') {
      setScannedProducts([...scannedProducts, product]);
      toast({
        title: "✓ Producto agregado",
        description: `${product.model} - ${product.storage} escaneado correctamente`,
      });
    } else {
      // En modo pricing, seleccionamos el producto para configurar precio
      setSelectedProductId(product.id.toString());
      setSalePrice(product.costPrice);
    }
    
    setImeiInput("");
  };

  const handleAddProduct = () => {
    if (!selectedProductId || !salePrice || !paymentMethod) {
      toast({
        title: "Error",
        description: "Debes seleccionar un producto, ingresar el precio de venta y seleccionar forma de pago",
        variant: "destructive",
      });
      return;
    }

    const product = scannedProducts?.find((p: Product) => p.id === parseInt(selectedProductId));
    if (!product) return;

    let amountUsd = salePrice;
    let exchangeRate = undefined;

    if (paymentMethod === "efectivo_pesos" || paymentMethod === "transferencia_pesos") {
      exchangeRate = exchangeRates[paymentMethod as keyof typeof exchangeRates]?.toString();
      amountUsd = salePrice; // Already in USD
    } else if (paymentMethod === "transferencia_financiera") {
      exchangeRate = exchangeRates.transferencia_financiera.toString();
      amountUsd = salePrice;
    } else if (paymentMethod === "financiera") {
      exchangeRate = exchangeRates.financiera.toString();
      amountUsd = salePrice; // Will be updated via swap if needed
    }

    const newItem: OrderItem = {
      productId: product.id,
      product,
      salePrice: parseFloat(salePrice).toFixed(2),
      paymentMethod,
      exchangeRate,
      amountUsd: parseFloat(amountUsd).toFixed(2),
    };

    setSelectedItems([...selectedItems, newItem]);
    
    // Remover el producto de la lista de escaneados para que no se pueda agregar de nuevo
    setScannedProducts(scannedProducts.filter(p => p.id !== product.id));
    
    // Limpiar la selección
    setSelectedProductId("");
    setSalePrice("");
    setPaymentMethod("");
    
    toast({
      title: "✓ Producto agregado",
      description: `${product.model} - ${product.storage} agregado al pedido`,
    });
  };

  const handleRemoveProduct = (productId: number) => {
    setSelectedItems(selectedItems.filter(item => item.productId !== productId));
  };

  const handleRemoveScannedProduct = (productId: number) => {
    setScannedProducts(scannedProducts.filter(product => product.id !== productId));
    // Si estaba seleccionado, limpiamos la selección
    if (selectedProductId === productId.toString()) {
      setSelectedProductId("");
      setSalePrice("");
      setPaymentMethod("");
    }
  };

  const handleClearScannedProducts = () => {
    setScannedProducts([]);
    setSelectedProductId("");
    setSalePrice("");
    setPaymentMethod("");
    setProductPrices({});
    setScanningMode('scanning');
  };

  const handleAddAllProducts = () => {
    const newItems: OrderItem[] = [];
    
    for (const product of scannedProducts) {
      const price = productPrices[product.id];
      if (price && parseFloat(price) > 0 && !selectedItems.some(item => item.productId === product.id)) {
        newItems.push({
          productId: product.id,
          product,
          salePrice: parseFloat(price).toFixed(2),
          paymentMethod: "efectivo_dolar", // Método temporal
          amountUsd: parseFloat(price).toFixed(2),
        });
      }
    }
    
    if (newItems.length > 0) {
      setSelectedItems([...selectedItems, ...newItems]);
      toast({
        title: "✓ Productos agregados",
        description: `${newItems.length} productos agregados al pedido`,
      });
    }
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!orderData.customerName.trim()) {
      toast({
        title: "Error",
        description: "El nombre del cliente es requerido",
        variant: "destructive",
      });
      return;
    }

    if (selectedItems.length === 0) {
      toast({
        title: "Error",
        description: "Debes agregar al menos un producto al pedido",
        variant: "destructive",
      });
      return;
    }

    if (!paymentStatus || orderPaymentMethods.length === 0) {
      toast({
        title: "Error",
        description: "Debes completar los métodos de pago y estado para guardar el pedido",
        variant: "destructive",
      });
      return;
    }

    // Validar que todos los métodos tengan montos
    const hasEmptyAmounts = orderPaymentMethods.some(method => 
      !paymentAmounts[method] || parseFloat(paymentAmounts[method]) <= 0
    );
    
    if (hasEmptyAmounts) {
      toast({
        title: "Error",
        description: "Debes completar los montos para todos los métodos de pago seleccionados",
        variant: "destructive",
      });
      return;
    }

    createOrderMutation.mutate({
      ...orderData,
      paymentStatus,
      paymentMethods: orderPaymentMethods,
      paymentAmounts,
      items: selectedItems
    });
  };

  const totalAmount = selectedItems.reduce((sum, item) => sum + parseFloat(item.amountUsd || item.salePrice), 0);

  const handleSwapComplete = (swapData: {
    fromCurrency: string;
    toCurrency: string;
    fromAmount: number;
    toAmount: number;
    exchangeRate: number;
  }) => {
    if (currentItemIndex >= 0 && currentItemIndex < selectedItems.length) {
      const updatedItems = [...selectedItems];
      if (updatedItems[currentItemIndex]) {
        updatedItems[currentItemIndex].exchangeRate = swapData.exchangeRate.toString();
        updatedItems[currentItemIndex].amountUsd = swapData.toCurrency === "USD" 
          ? swapData.toAmount.toString() 
          : swapData.fromAmount.toString();
        
        setSelectedItems(updatedItems);
      }
    }
    setShowSwap(false);
    setCurrentItemIndex(-1);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[1000px] max-h-[90vh] overflow-y-auto pb-0">
        <DialogHeader>
          <DialogTitle>Crear Pedido - Escáner IMEI</DialogTitle>
        </DialogHeader>
        
        <form onSubmit={handleSubmit} className="space-y-6 pb-16">
          {/* Información del cliente */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Información del Cliente</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div>
                <Label htmlFor="customerId">Cliente Existente</Label>
                <Select 
                  value={orderData.customerId} 
                  onValueChange={(value) => {
                    if (value === "new") {
                      setOrderData({
                        ...orderData,
                        customerId: "",
                        customerName: "",
                        customerPhone: "",
                        customerEmail: "",
                      });
                    } else {
                      const selectedCustomer = customers?.find(c => c.id.toString() === value);
                      if (selectedCustomer) {
                        setOrderData({
                          ...orderData,
                          customerId: value,
                          customerName: selectedCustomer.name,
                          customerPhone: selectedCustomer.phone || "",
                          customerEmail: selectedCustomer.email || "",
                        });
                      }
                    }
                  }}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Seleccionar cliente existente" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="new">Nuevo cliente</SelectItem>
                    {customers?.map((customer) => (
                      <SelectItem key={customer.id} value={customer.id.toString()}>
                        {customer.name} - {customer.phone}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div>
                <Label htmlFor="vendorId">Vendedor</Label>
                <Select 
                  value={orderData.vendorId} 
                  onValueChange={(value) => setOrderData({ ...orderData, vendorId: value })}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Seleccionar vendedor" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="none">Sin vendedor asignado</SelectItem>
                    {vendors?.map((vendor) => (
                      <SelectItem key={vendor.id} value={vendor.id.toString()}>
                        {vendor.name} - {vendor.phone}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div>
                <Label htmlFor="customerName">Nombre del Cliente *</Label>
                <Input
                  id="customerName"
                  value={orderData.customerName}
                  onChange={(e) => setOrderData({ ...orderData, customerName: e.target.value })}
                  placeholder="Ej: Juan Pérez"
                  required
                />
              </div>
              
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <Label htmlFor="customerPhone">Teléfono</Label>
                  <Input
                    id="customerPhone"
                    value={orderData.customerPhone}
                    onChange={(e) => setOrderData({ ...orderData, customerPhone: e.target.value })}
                    placeholder="Ej: +54 9 11 1234-5678"
                  />
                </div>
                
                <div>
                  <Label htmlFor="customerEmail">Email</Label>
                  <Input
                    id="customerEmail"
                    type="email"
                    value={orderData.customerEmail}
                    onChange={(e) => setOrderData({ ...orderData, customerEmail: e.target.value })}
                    placeholder="Ej: juan@ejemplo.com"
                  />
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Escáner IMEI */}
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle className="text-lg">Escanear IMEI</CardTitle>
                <div className="flex gap-2">
                  <Button
                    type="button"
                    variant={scanningMode === 'scanning' ? "default" : "outline"}
                    size="sm"
                    onClick={() => setScanningMode('scanning')}
                  >
                    Escaneo Continuo
                  </Button>
                  <Button
                    type="button"
                    variant={scanningMode === 'pricing' ? "default" : "outline"}
                    size="sm"
                    onClick={() => setScanningMode('pricing')}
                    disabled={scannedProducts.length === 0}
                  >
                    Configurar Precios
                  </Button>
                </div>
              </div>
            </CardHeader>
            <CardContent className="space-y-4">
              {scanningMode === 'scanning' ? (
                <div className="space-y-4">
                  <div>
                    <Label htmlFor="product-imei">Escanear IMEI - Modo Continuo</Label>
                    <Input
                      id="product-imei"
                      type="text"
                      value={imeiInput}
                      onChange={(e) => {
                        setImeiInput(e.target.value);
                        // Auto-buscar cuando el IMEI tiene longitud suficiente
                        if (e.target.value.length >= 15) {
                          handleScanIMEI();
                        }
                      }}
                      placeholder="Escanear IMEI del producto (automático al completar)"
                      className="font-mono text-lg"
                      onKeyPress={(e) => {
                        if (e.key === 'Enter') {
                          e.preventDefault();
                          handleScanIMEI();
                        }
                      }}
                      autoComplete="off"
                      autoFocus
                    />
                    <p className="text-sm text-gray-500 mt-1">
                      Escaneá uno tras otro. Al finalizar, cambiá a "Configurar Precios"
                    </p>
                  </div>
                  
                  {scannedProducts.length > 0 && (
                    <div className="space-y-3">
                      <div className="p-3 bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md">
                        <div className="flex items-center justify-between">
                          <div>
                            <div className="font-medium text-green-800 dark:text-green-200">
                              ✓ {scannedProducts.length} producto{scannedProducts.length > 1 ? 's' : ''} escaneado{scannedProducts.length > 1 ? 's' : ''}
                            </div>
                            <div className="text-sm text-green-600 dark:text-green-300 mt-1">
                              Listos para configurar precios de venta
                            </div>
                          </div>
                          <Button
                            type="button"
                            variant="outline"
                            size="sm"
                            onClick={handleClearScannedProducts}
                            className="text-red-600 hover:text-red-700"
                          >
                            Limpiar Todo
                          </Button>
                        </div>
                      </div>
                      
                      <div className="space-y-2">
                        {scannedProducts.map((product) => (
                          <div key={product.id} className="flex items-center justify-between p-2 bg-gray-50 dark:bg-gray-800 rounded border">
                            <div className="flex-1">
                              <div className="font-medium text-gray-900 dark:text-white">
                                {product.model} - {product.storage} - {product.color}
                              </div>
                              <div className="text-sm text-gray-500 dark:text-gray-400">
                                IMEI: {product.imei} | Costo: ${product.costPrice}
                              </div>
                            </div>
                            <Button
                              type="button"
                              variant="ghost"
                              size="sm"
                              onClick={() => handleRemoveScannedProduct(product.id)}
                              className="text-red-500 hover:text-red-700"
                            >
                              <Trash2 className="h-4 w-4" />
                            </Button>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              ) : (
                <div className="space-y-4">
                  <div className="p-3 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-md">
                    <div className="font-medium text-blue-800 dark:text-blue-200 mb-2">
                      📝 PASO 2: Resumen Automático de Modelos
                    </div>
                    <div className="text-sm text-blue-600 dark:text-blue-300">
                      • Modelo, Almacenamiento, Estado y IMEI mostrados
                      • Usa Enter para navegar entre campos de precio
                      • Configura el precio de venta para cada producto
                    </div>
                  </div>

                  {/* Lista de productos para configurar precios */}
                  <div className="space-y-3">
                    {scannedProducts.map((product, index) => {
                      const isConfigured = selectedItems.some(item => item.productId === product.id);
                      return (
                        <div 
                          key={product.id} 
                          className={`p-4 border rounded-lg ${
                            isConfigured 
                              ? 'bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-800' 
                              : 'bg-white dark:bg-gray-800 border-gray-200 dark:border-gray-700'
                          }`}
                        >
                          <div className="flex justify-between items-start mb-3">
                            <div className="flex-1">
                              <div className="font-medium text-gray-900 dark:text-white">
                                📱 {product.model}
                              </div>
                              <div className="text-sm text-gray-600 dark:text-gray-300 mt-1">
                                💾 Almacenamiento: {product.storage}
                              </div>
                              <div className="text-sm text-gray-600 dark:text-gray-300">
                                🔖 Estado: <span className="font-medium capitalize">{product.status}</span>
                              </div>
                              <div className="text-sm text-gray-500 dark:text-gray-400">
                                🔢 IMEI: {product.imei} | 💰 Costo: ${product.costPrice}
                              </div>
                              {isConfigured && (
                                <div className="text-sm text-green-600 dark:text-green-400 font-medium mt-1">
                                  ✅ Configurado y agregado al pedido
                                </div>
                              )}
                            </div>
                            {!isConfigured && (
                              <Button
                                type="button"
                                variant="ghost"
                                size="sm"
                                onClick={() => handleRemoveScannedProduct(product.id)}
                                className="text-red-500 hover:text-red-700"
                              >
                                <Trash2 className="h-4 w-4" />
                              </Button>
                            )}
                          </div>
                          
                          {!isConfigured && (
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                              <div>
                                <Label htmlFor={`price-${product.id}`} className="text-sm font-medium">
                                  Precio de Venta (USD)
                                </Label>
                                <Input
                                  id={`price-${product.id}`}
                                  type="number"
                                  placeholder="0.00"
                                  defaultValue={product.costPrice}
                                  value={productPrices[product.id] || ''}
                                  onChange={(e) => {
                                    setProductPrices({
                                      ...productPrices,
                                      [product.id]: e.target.value
                                    });
                                  }}
                                  onKeyDown={(e) => {
                                    if (e.key === 'Enter') {
                                      e.preventDefault();
                                      // Buscar el siguiente campo de precio sin configurar
                                      const nextProduct = scannedProducts.find((p, i) => 
                                        i > index && !selectedItems.some(item => item.productId === p.id)
                                      );
                                      if (nextProduct) {
                                        const nextInput = document.getElementById(`price-${nextProduct.id}`) as HTMLInputElement;
                                        nextInput?.focus();
                                      } else {
                                        // Si es el último, agregar todos los productos
                                        handleAddAllProducts();
                                      }
                                    }
                                  }}
                                  className="mt-1"
                                  min="0"
                                  step="0.01"
                                  autoFocus={index === 0}
                                />
                              </div>
                              
                              <div className="flex items-end">
                                <Button
                                  type="button"
                                  onClick={() => {
                                    const price = productPrices[product.id];
                                    if (price && parseFloat(price) > 0) {
                                      // Agregar producto individual
                                      const newItem: OrderItem = {
                                        productId: product.id,
                                        product,
                                        salePrice: parseFloat(price).toFixed(2),
                                        paymentMethod: "efectivo_dolar", // Método temporal, se seleccionará al final
                                        amountUsd: parseFloat(price).toFixed(2),
                                      };
                                      setSelectedItems([...selectedItems, newItem]);
                                      
                                      toast({
                                        title: "✓ Producto agregado",
                                        description: `${product.model} - ${product.storage} agregado al pedido`,
                                      });
                                    } else {
                                      toast({
                                        title: "Error",
                                        description: "Ingresa un precio válido",
                                        variant: "destructive",
                                      });
                                    }
                                  }}
                                  className="w-full bg-green-600 hover:bg-green-700"
                                  disabled={!productPrices[product.id] || parseFloat(productPrices[product.id] || '0') <= 0}
                                >
                                  <Plus className="mr-1 h-4 w-4" />
                                  Agregar Producto
                                </Button>
                              </div>
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                  
                  {/* Total de productos escaneados */}
                  {scannedProducts.length > 0 && (
                    <div className="p-4 bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg">
                      <div className="flex justify-between items-center">
                        <div>
                          <div className="text-sm text-gray-600 dark:text-gray-400">
                            Progreso: {selectedItems.length} de {scannedProducts.length} configurados
                          </div>
                          <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2 mt-1">
                            <div 
                              className="bg-green-600 h-2 rounded-full transition-all duration-300" 
                              style={{ width: `${(selectedItems.length / scannedProducts.length) * 100}%` }}
                            ></div>
                          </div>
                        </div>
                        <div className="text-right ml-4">
                          <div className="text-sm text-gray-600 dark:text-gray-400">Total Estimado</div>
                          <div className="text-lg font-bold text-green-600">
                            ${totalAmount.toFixed(2)} USD
                          </div>
                        </div>
                      </div>
                      
                      {Object.keys(productPrices).length > 0 && scannedProducts.length > 0 && (
                        <div className="mt-3 pt-3 border-t">
                          <Button
                            type="button"
                            onClick={handleAddAllProducts}
                            className="w-full bg-blue-600 hover:bg-blue-700"
                            disabled={scannedProducts.some(p => !productPrices[p.id] || parseFloat(productPrices[p.id] || '0') <= 0)}
                          >
                            <Plus className="mr-2 h-4 w-4" />
                            Agregar Todos los Productos al Pedido
                          </Button>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              )}

              {/* Información del producto encontrado */}
              {scanningMode === 'pricing' && selectedProductId && (
                <div className="p-3 bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md">
                  <div className="font-medium text-green-800 dark:text-green-200">
                    ✓ Producto encontrado: {scannedProducts?.find(p => p.id === parseInt(selectedProductId))?.model} - {scannedProducts?.find(p => p.id === parseInt(selectedProductId))?.storage} - {scannedProducts?.find(p => p.id === parseInt(selectedProductId))?.color}
                  </div>
                  <div className="text-sm text-green-600 dark:text-green-300 mt-1">
                    IMEI: {scannedProducts?.find(p => p.id === parseInt(selectedProductId))?.imei} | Costo: ${scannedProducts?.find(p => p.id === parseInt(selectedProductId))?.costPrice}
                  </div>
                </div>
              )}

              {/* Mostrar conversión si es Transferencia Financiera o Financiera */}
              {scanningMode === 'pricing' && (paymentMethod === "transferencia_financiera" || paymentMethod === "financiera") && selectedProductId && (
                <div className="p-3 bg-blue-50 border border-blue-200 rounded-md">
                  <div className="font-medium text-blue-800">
                    💱 Conversión {paymentMethod === "financiera" ? "Financiera" : "Transferencia Financiera"}
                  </div>
                  <div className="text-sm text-blue-600 mt-1">
                    Precio USD: ${salePrice} | Tasa: {paymentMethod === "financiera" ? exchangeRates.financiera : exchangeRates.transferencia_financiera} | Pesos: ${(parseFloat(salePrice) * (paymentMethod === "financiera" ? exchangeRates.financiera : exchangeRates.transferencia_financiera)).toFixed(2)}
                  </div>
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    className="mt-2"
                    onClick={() => {
                      // Solo abrir si hay items seleccionados
                      if (selectedItems.length > 0) {
                        setCurrentItemIndex(selectedItems.length - 1); // Usar el último item
                        setShowSwap(true);
                      }
                    }}
                  >
                    <Calculator className="h-4 w-4 mr-1" />
                    Ajustar Conversión
                  </Button>
                </div>
              )}
            </CardContent>
          </Card>





          {/* Lista de productos en el pedido */}
          {selectedItems.length > 0 && (
            <Card>
              <CardHeader>
                <CardTitle className="text-lg flex items-center justify-between">
                  <div className="flex items-center">
                    <ShoppingCart className="mr-2 h-5 w-5" />
                    Productos en el Pedido ({selectedItems.length})
                  </div>
                  <div className="text-xl font-bold text-green-600">
                    ${totalAmount.toFixed(2)} USD
                  </div>
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-2">
                  {selectedItems.map((item) => (
                    <div key={item.productId} className="flex items-center justify-between p-3 bg-gray-50 dark:bg-gray-800 rounded">
                      <div className="flex-1">
                        <div className="font-medium">
                          {item.product.model} - {item.product.storage} - {item.product.color}
                        </div>
                        <div className="text-sm text-gray-500">
                          IMEI: {item.product.imei} | Costo: ${item.product.costPrice}
                        </div>
                        <div className="text-sm text-blue-600 mt-1">
                          Pago: {paymentMethods.find(m => m.value === item.paymentMethod)?.label}
                        </div>
                        {(item.paymentMethod === "transferencia_financiera" || 
                          item.paymentMethod === "efectivo_pesos" || 
                          item.paymentMethod === "transferencia_pesos") && (
                          <div className="text-sm text-gray-600 mt-1">
                            {item.paymentMethod === "transferencia_financiera" && item.exchangeRate && (
                              <>Tasa: {item.exchangeRate} | USD: ${item.amountUsd}</>
                            )}
                            {(item.paymentMethod === "efectivo_pesos" || item.paymentMethod === "transferencia_pesos") && (
                              <>Tasa: {exchangeRates[item.paymentMethod as keyof typeof exchangeRates]} | USD: ${item.amountUsd}</>
                            )}
                          </div>
                        )}
                      </div>
                      <div className="flex items-center space-x-2">
                        {item.paymentMethod === "transferencia_financiera" && (
                          <Button
                            type="button"
                            variant="outline"
                            size="sm"
                            onClick={() => {
                              const itemIndex = selectedItems.findIndex(i => i.productId === item.productId);
                              if (itemIndex >= 0) {
                                setCurrentItemIndex(itemIndex);
                                setShowSwap(true);
                              }
                            }}
                          >
                            <Calculator className="h-4 w-4 mr-1" />
                            Cotizar
                          </Button>
                        )}
                        <span className="font-medium text-green-600">
                          ${item.salePrice}
                        </span>
                        <Button
                          type="button"
                          variant="ghost"
                          size="sm"
                          onClick={() => handleRemoveProduct(item.productId)}
                          className="text-red-500 hover:text-red-700"
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </div>
                    </div>
                  ))}
                </div>
                

              </CardContent>
            </Card>
          )}

          {/* PASO 5: Métodos de Pago y Estado OBLIGATORIO */}
          {selectedItems.length > 0 && (
            <Card className="border-orange-200 bg-orange-50 dark:bg-orange-900/20 dark:border-orange-800">
              <CardHeader>
                <CardTitle className="text-lg text-orange-700 dark:text-orange-300 flex items-center">
                  🛑 Paso 5: Forma de Pago del Pedido (OBLIGATORIO)
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div>
                    <Label className="text-sm font-medium mb-2 block">Métodos de Pago para el Pedido Completo</Label>
                    <div className="grid grid-cols-2 md:grid-cols-3 gap-2">
                      {paymentMethods.map((method) => (
                        <label key={method.value} className="flex items-center space-x-2 p-2 border rounded cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700">
                          <input
                            type="checkbox"
                            checked={orderPaymentMethods.includes(method.value)}
                            onChange={(e) => {
                              if (e.target.checked) {
                                setOrderPaymentMethods([...orderPaymentMethods, method.value]);
                                // Inicializar el monto para este método
                                setPaymentAmounts({
                                  ...paymentAmounts,
                                  [method.value]: ''
                                });
                              } else {
                                setOrderPaymentMethods(orderPaymentMethods.filter(m => m !== method.value));
                                // Remover el monto de este método
                                const newAmounts = {...paymentAmounts};
                                delete newAmounts[method.value];
                                setPaymentAmounts(newAmounts);
                              }
                            }}
                            className="rounded"
                          />
                          <span className="text-sm">{method.label}</span>
                        </label>
                      ))}
                    </div>
                    {orderPaymentMethods.length === 0 && (
                      <p className="text-sm text-red-600 dark:text-red-400 mt-1">
                        Selecciona al menos un método de pago
                      </p>
                    )}
                  </div>

                  {/* Campos de cantidad por método de pago */}
                  {orderPaymentMethods.length > 0 && (
                    <div className="space-y-3">
                      <Label className="text-sm font-medium">Montos por Método de Pago</Label>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                        {orderPaymentMethods.map((methodValue) => {
                          const method = paymentMethods.find(m => m.value === methodValue);
                          return (
                            <div key={methodValue} className="space-y-1">
                              <Label className="text-xs text-gray-600 dark:text-gray-400">
                                {method?.label}
                              </Label>
                              <Input
                                type="number"
                                placeholder="0.00"
                                min="0"
                                step="0.01"
                                value={paymentAmounts[methodValue] || ''}
                                onChange={(e) => {
                                  setPaymentAmounts({
                                    ...paymentAmounts,
                                    [methodValue]: e.target.value
                                  });
                                }}
                                className="h-8"
                              />
                            </div>
                          );
                        })}
                      </div>
                      
                      {/* Validación de montos */}
                      {(() => {
                        const totalPaid = orderPaymentMethods.reduce((sum, method) => {
                          return sum + (parseFloat(paymentAmounts[method] || '0') || 0);
                        }, 0);
                        const totalOrder = totalAmount;
                        const hasEmptyAmounts = orderPaymentMethods.some(method => 
                          !paymentAmounts[method] || parseFloat(paymentAmounts[method]) <= 0
                        );
                        
                        return (
                          <div className="p-2 bg-gray-50 dark:bg-gray-800 rounded text-sm">
                            <div className="flex justify-between">
                              <span>Total del Pedido:</span>
                              <span className="font-medium">${totalOrder.toFixed(2)} USD</span>
                            </div>
                            <div className="flex justify-between">
                              <span>Total Pagado:</span>
                              <span className={`font-medium ${totalPaid === totalOrder ? 'text-green-600' : totalPaid > totalOrder ? 'text-orange-600' : 'text-red-600'}`}>
                                ${totalPaid.toFixed(2)} USD
                              </span>
                            </div>
                            {hasEmptyAmounts && (
                              <div className="text-red-600 dark:text-red-400 mt-1">
                                Completa todos los montos de pago
                              </div>
                            )}
                            {totalPaid !== totalOrder && !hasEmptyAmounts && (
                              <div className="text-orange-600 dark:text-orange-400 mt-1">
                                {totalPaid > totalOrder ? 'Monto mayor al total' : 'Monto menor al total'}
                              </div>
                            )}
                          </div>
                        );
                      })()}
                    </div>
                  )}

                  <div>
                    <Label className="text-sm font-medium">Estado del Pago *</Label>
                    <Select value={paymentStatus} onValueChange={setPaymentStatus}>
                      <SelectTrigger className="mt-1">
                        <SelectValue placeholder="Seleccionar estado del pago" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="total">Pago Total</SelectItem>
                        <SelectItem value="parcial">Pago Parcial</SelectItem>
                        <SelectItem value="no_pagado">No Pagado</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>

                  <div>
                    <Label className="text-sm font-medium">Observaciones (Opcional)</Label>
                    <textarea
                      value={observations}
                      onChange={(e) => setObservations(e.target.value)}
                      placeholder="Notas adicionales sobre el pedido..."
                      rows={3}
                      className="mt-1 w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-white resize-none focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                  </div>
                  
                  {(paymentStatus && orderPaymentMethods.length > 0) && (
                    <div className="p-3 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded">
                      <div className="text-sm font-medium text-gray-900 dark:text-white">
                        Resumen del Pedido:
                      </div>
                      <div className="text-sm text-gray-600 dark:text-gray-400 mt-1">
                        • {selectedItems.length} producto{selectedItems.length > 1 ? 's' : ''} agregado{selectedItems.length > 1 ? 's' : ''}
                      </div>
                      <div className="text-sm text-gray-600 dark:text-gray-400">
                        • Total: ${totalAmount.toFixed(2)} USD
                      </div>
                      <div className="text-sm text-gray-600 dark:text-gray-400">
                        • Métodos: {orderPaymentMethods.map(m => paymentMethods.find(pm => pm.value === m)?.label).join(', ')}
                      </div>
                      <div className="text-sm font-medium mt-2 text-gray-900 dark:text-white">
                        Estado: {paymentStatus === "total" ? "Pago Total" : paymentStatus === "parcial" ? "Pago Parcial" : "No Pagado"}
                      </div>
                    </div>
                  )}
                  
                  {(!paymentStatus || orderPaymentMethods.length === 0) && (
                    <div className="p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded text-center">
                      <div className="text-sm text-red-700 dark:text-red-300 font-medium">
                        ⚠️ Debe completar los métodos de pago y estado para guardar el pedido
                      </div>
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>
          )}

          {/* PASO 6: Tipo de Envío */}
          {selectedItems.length > 0 && paymentStatus && orderPaymentMethods.length > 0 && (
            <Card className="border-purple-200 bg-purple-50 dark:bg-purple-900/20 dark:border-purple-800">
              <CardHeader>
                <CardTitle className="text-lg text-purple-700 dark:text-purple-300 flex items-center">
                  🚚 Paso 6: Tipo de Envío
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div>
                    <Label className="text-sm font-medium mb-2 block">Selecciona el tipo de envío</Label>
                    <div className="grid grid-cols-2 gap-3">
                      <label className={`flex items-center space-x-3 p-3 border rounded cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700 ${shippingType === 'oficina' ? 'bg-purple-100 dark:bg-purple-900/30 border-purple-300 dark:border-purple-600' : 'border-gray-200 dark:border-gray-600'}`}>
                        <input
                          type="radio"
                          name="shippingType"
                          value="oficina"
                          checked={shippingType === 'oficina'}
                          onChange={(e) => {
                            setShippingType(e.target.value as "oficina");
                            setShippingAddress(""); // Limpiar dirección si se selecciona oficina
                          }}
                          className="rounded"
                        />
                        <div>
                          <div className="font-medium text-gray-900 dark:text-white">🏢 Oficina</div>
                          <div className="text-sm text-gray-500 dark:text-gray-400">Retiro en oficina</div>
                        </div>
                      </label>
                      
                      <label className={`flex items-center space-x-3 p-3 border rounded cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700 ${shippingType === 'direccion' ? 'bg-purple-100 dark:bg-purple-900/30 border-purple-300 dark:border-purple-600' : 'border-gray-200 dark:border-gray-600'}`}>
                        <input
                          type="radio"
                          name="shippingType"
                          value="direccion"
                          checked={shippingType === 'direccion'}
                          onChange={(e) => setShippingType(e.target.value as "direccion")}
                          className="rounded"
                        />
                        <div>
                          <div className="font-medium text-gray-900 dark:text-white">🏠 Dirección</div>
                          <div className="text-sm text-gray-500 dark:text-gray-400">Envío a domicilio</div>
                        </div>
                      </label>
                    </div>
                  </div>

                  {/* Campo de dirección cuando se selecciona "Dirección" */}
                  {shippingType === 'direccion' && (
                    <div>
                      <Label className="text-sm font-medium">Dirección de Envío *</Label>
                      <textarea
                        value={shippingAddress}
                        onChange={(e) => setShippingAddress(e.target.value)}
                        placeholder="Ingresa la dirección completa de envío..."
                        rows={3}
                        className="mt-1 w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-white resize-none focus:outline-none focus:ring-2 focus:ring-purple-500"
                      />
                      {shippingType === 'direccion' && !shippingAddress.trim() && (
                        <p className="text-sm text-red-600 dark:text-red-400 mt-1">
                          La dirección de envío es obligatoria cuando se selecciona "Dirección"
                        </p>
                      )}
                    </div>
                  )}

                  {shippingType && (
                    <div className="p-3 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded">
                      <div className="text-sm font-medium text-gray-900 dark:text-white">
                        Tipo de Envío Seleccionado:
                      </div>
                      <div className="text-sm text-gray-600 dark:text-gray-400 mt-1">
                        • {shippingType === 'oficina' ? '🏢 Retiro en Oficina' : '🏠 Envío a Domicilio'}
                      </div>
                      {shippingType === 'direccion' && shippingAddress && (
                        <div className="text-sm text-gray-600 dark:text-gray-400">
                          • Dirección: {shippingAddress}
                        </div>
                      )}
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>
          )}

          {/* Botones */}
          <div className="flex justify-end space-x-2 pt-4 border-t bg-white dark:bg-gray-900 sticky bottom-0 mt-4">
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
              Cancelar
            </Button>
            <Button 
              type="submit" 
              disabled={
                createOrderMutation.isPending || 
                selectedItems.length === 0 || 
                !paymentStatus || 
                orderPaymentMethods.length === 0 ||
                orderPaymentMethods.some(method => !paymentAmounts[method] || parseFloat(paymentAmounts[method]) <= 0) ||
                !shippingType ||
                (shippingType === 'direccion' && !shippingAddress.trim())
              }
              className={(
                !paymentStatus || 
                orderPaymentMethods.length === 0 ||
                orderPaymentMethods.some(method => !paymentAmounts[method] || parseFloat(paymentAmounts[method]) <= 0) ||
                !shippingType ||
                (shippingType === 'direccion' && !shippingAddress.trim())
              ) ? "opacity-50 cursor-not-allowed" : ""}
            >
              {createOrderMutation.isPending ? "Creando..." : (
                !paymentStatus || orderPaymentMethods.length === 0 ? "Completar Métodos y Estado" : 
                orderPaymentMethods.some(method => !paymentAmounts[method] || parseFloat(paymentAmounts[method]) <= 0) ? "Completar Montos de Pago" :
                !shippingType ? "Seleccionar Tipo de Envío" :
                (shippingType === 'direccion' && !shippingAddress.trim()) ? "Completar Dirección de Envío" :
                "Crear Pedido"
              )}
            </Button>
          </div>
        </form>

        {/* Currency Swap Modal */}
        {showSwap && (
          <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
            <div className="bg-white dark:bg-gray-800 rounded-lg p-4 max-w-sm w-full max-h-[90vh] overflow-y-auto">
              <div className="flex items-center justify-between mb-3">
                <h2 className="text-base font-semibold text-gray-900 dark:text-white">Conversión</h2>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => setShowSwap(false)}
                  className="text-gray-500 hover:text-gray-700 h-6 w-6 p-0"
                >
                  ×
                </Button>
              </div>
              <CurrencySwap onSwapComplete={handleSwapComplete} />
            </div>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}