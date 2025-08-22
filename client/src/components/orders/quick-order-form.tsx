import { useState, useRef, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useAuth } from "@/lib/auth";
import { apiRequest } from "@/lib/queryClient";
import { toast } from "@/hooks/use-toast";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { Plus, Trash2, ShoppingCart } from "lucide-react";
import type { Product, Customer, Vendor } from "@shared/schema";

interface QuickOrderFormProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

interface OrderItem {
  productId: number;
  product: Product;
  salePrice: string;
  amountUsd: string;
}

export function QuickOrderForm({ open, onOpenChange }: QuickOrderFormProps) {
  const { user } = useAuth();
  const queryClient = useQueryClient();
  
  // Estados principales
  const [currentStep, setCurrentStep] = useState<'scan' | 'customer' | 'payment'>('scan');
  const [scannedProducts, setScannedProducts] = useState<Product[]>([]);
  const [selectedItems, setSelectedItems] = useState<OrderItem[]>([]);
  const [imeiInput, setImeiInput] = useState("");
  const [customerInput, setCustomerInput] = useState("");
  const [paymentMethods, setPaymentMethods] = useState<string[]>([]);
  const [paymentAmounts, setPaymentAmounts] = useState<{[key: string]: string}>({});
  const [observations, setObservations] = useState("");
  const [paymentStatus, setPaymentStatus] = useState<"total" | "parcial" | "no_pagado">("total");
  
  // Referencias para navegación con teclado
  const imeiRef = useRef<HTMLInputElement>(null);
  const customerRef = useRef<HTMLInputElement>(null);
  const firstPaymentRef = useRef<HTMLInputElement>(null);
  
  // Datos
  const { data: products } = useQuery({
    queryKey: ['/api/products', user?.clientId],
    enabled: !!user?.clientId,
  });

  const { data: customers } = useQuery({
    queryKey: ['/api/customers', user?.clientId],
    enabled: !!user?.clientId,
  });

  const { data: vendors } = useQuery({
    queryKey: ['/api/vendors', user?.clientId],
    enabled: !!user?.clientId,
  });

  const availablePaymentMethods = [
    { key: 'efectivo_dolar', label: 'Efectivo Dólar', shortcut: '1' },
    { key: 'efectivo_pesos', label: 'Efectivo Pesos', shortcut: '2' },
    { key: 'transferencia_pesos', label: 'Transferencia Pesos', shortcut: '3' },
    { key: 'transferencia_usdt', label: 'Transferencia USDT', shortcut: '4' },
    { key: 'transferencia_financiera', label: 'Transferencia Financiera', shortcut: '5' },
    { key: 'financiera', label: 'Financiera', shortcut: '6' },
  ];

  // Auto-focus
  useEffect(() => {
    if (!open) return;
    
    if (currentStep === 'scan') {
      setTimeout(() => imeiRef.current?.focus(), 100);
    } else if (currentStep === 'customer') {
      setTimeout(() => customerRef.current?.focus(), 100);
    } else if (currentStep === 'payment') {
      setTimeout(() => firstPaymentRef.current?.focus(), 100);
    }
  }, [currentStep, open]);

  // Mutación para crear pedido
  const createOrderMutation = useMutation({
    mutationFn: async (data: any) => {
      const orderNumber = `ORD-${Date.now()}`;
      const totalUsd = selectedItems.reduce((sum, item) => sum + parseFloat(item.salePrice), 0);
      
      const order = await apiRequest('POST', '/api/orders', {
        clientId: user?.clientId,
        orderNumber,
        customerName: data.customerName,
        totalUsd: totalUsd.toFixed(2),
        status: 'pendiente',
        paymentStatus,
        observations: observations || null,
        vendorId: user?.id,
      });

      for (const item of selectedItems) {
        await apiRequest('POST', '/api/order-items', {
          orderId: order.id,
          productId: item.productId,
          quantity: 1,
          priceUsd: parseFloat(item.salePrice),
          paymentMethod: paymentMethods.join(', '),
          amountUsd: parseFloat(item.salePrice),
        });

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
        title: "✓ Pedido creado",
        description: `Pedido de ${selectedItems.length} productos completado`,
      });
      
      resetForm();
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

  const resetForm = () => {
    setScannedProducts([]);
    setSelectedItems([]);
    setImeiInput("");
    setCustomerInput("");
    setPaymentMethods([]);
    setPaymentAmounts({});
    setObservations("");
    setPaymentStatus("total");
    setCurrentStep('scan');
  };

  // Funciones principales
  const handleScanIMEI = async (imei: string) => {
    if (!imei.trim()) return;

    const product = products?.find((p: Product) => p.imei === imei.trim());
    if (!product) {
      toast({
        title: "IMEI no encontrado",
        description: `No se encontró producto con IMEI: ${imei}`,
        variant: "destructive",
      });
      return;
    }

    if (!['disponible', 'reservado'].includes(product.status)) {
      toast({
        title: "Producto no disponible",
        description: `Estado actual: ${product.status}`,
        variant: "destructive",
      });
      return;
    }

    if (scannedProducts.some(p => p.id === product.id)) {
      toast({
        title: "Producto duplicado",
        description: "Este producto ya fue escaneado",
        variant: "destructive",
      });
      return;
    }

    setScannedProducts([...scannedProducts, product]);
    setImeiInput("");
    
    toast({
      title: "✓ Producto escaneado",
      description: `${product.model} - ${product.storage}`,
    });
  };

  const addProductToOrder = (product: Product, price: string) => {
    if (!price || parseFloat(price) <= 0) {
      toast({
        title: "Precio inválido",
        description: "Ingresa un precio válido",
        variant: "destructive",
      });
      return;
    }

    const newItem: OrderItem = {
      productId: product.id,
      product,
      salePrice: parseFloat(price).toFixed(2),
      amountUsd: parseFloat(price).toFixed(2),
    };

    setSelectedItems([...selectedItems, newItem]);
    setScannedProducts(scannedProducts.filter(p => p.id !== product.id));
    
    toast({
      title: "✓ Agregado al pedido",
      description: `${product.model} - $${price}`,
    });
  };

  const totalAmount = selectedItems.reduce((sum, item) => sum + parseFloat(item.salePrice), 0);

  // Manejo de teclas globales
  const handleGlobalKeyPress = (e: KeyboardEvent) => {
    if (!open) return;

    // F1-F6 para métodos de pago rápidos
    if (currentStep === 'payment') {
      const methodIndex = parseInt(e.key) - 1;
      if (methodIndex >= 0 && methodIndex < availablePaymentMethods.length) {
        const method = availablePaymentMethods[methodIndex];
        if (!paymentMethods.includes(method.key)) {
          setPaymentMethods([...paymentMethods, method.key]);
          setPaymentAmounts({
            ...paymentAmounts,
            [method.key]: ''
          });
        }
        e.preventDefault();
      }
    }

    // Enter para avanzar pasos
    if (e.key === 'Enter' && !e.shiftKey) {
      if (currentStep === 'scan' && selectedItems.length > 0) {
        setCurrentStep('customer');
      } else if (currentStep === 'customer' && customerInput.trim()) {
        setCurrentStep('payment');
      }
    }

    // Escape para cancelar
    if (e.key === 'Escape') {
      onOpenChange(false);
    }
  };

  useEffect(() => {
    document.addEventListener('keydown', handleGlobalKeyPress);
    return () => document.removeEventListener('keydown', handleGlobalKeyPress);
  }, [open, currentStep, selectedItems, customerInput, paymentMethods]);

  if (!open) return null;

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div className="bg-white dark:bg-gray-900 rounded-lg shadow-xl w-full max-w-4xl max-h-[90vh] overflow-y-auto">
        <div className="p-6">
          <div className="flex justify-between items-center mb-6">
            <h2 className="text-2xl font-bold">Pedido Rápido - Sin Mouse</h2>
            <Button
              variant="outline"
              onClick={() => onOpenChange(false)}
              className="text-xs"
            >
              ESC para cerrar
            </Button>
          </div>

          {/* Indicador de progreso */}
          <div className="flex mb-6 space-x-2">
            <div className={`flex-1 h-2 rounded ${currentStep === 'scan' ? 'bg-blue-500' : 'bg-gray-200'}`} />
            <div className={`flex-1 h-2 rounded ${currentStep === 'customer' ? 'bg-blue-500' : 'bg-gray-200'}`} />
            <div className={`flex-1 h-2 rounded ${currentStep === 'payment' ? 'bg-blue-500' : 'bg-gray-200'}`} />
          </div>

          {/* PASO 1: Escaneo */}
          {currentStep === 'scan' && (
            <Card>
              <CardContent className="p-6">
                <div className="space-y-4">
                  <div>
                    <Label className="text-lg font-medium">1. Escanear Productos (Enter para agregar)</Label>
                    <div className="flex space-x-2 mt-2">
                      <Input
                        ref={imeiRef}
                        placeholder="Escanear IMEI y presionar Enter..."
                        value={imeiInput}
                        onChange={(e) => setImeiInput(e.target.value)}
                        onKeyPress={(e) => {
                          if (e.key === 'Enter') {
                            handleScanIMEI(imeiInput);
                          }
                        }}
                        className="flex-1"
                        autoFocus
                      />
                    </div>
                  </div>

                  {/* Productos escaneados */}
                  {scannedProducts.length > 0 && (
                    <div className="space-y-2">
                      <Label className="font-medium">Productos Escaneados (Enter en precio para agregar)</Label>
                      {scannedProducts.map((product, index) => (
                        <div key={product.id} className="flex items-center space-x-2 p-3 bg-gray-50 dark:bg-gray-800 rounded">
                          <div className="flex-1">
                            <div className="font-medium">{product.model} - {product.storage}</div>
                            <div className="text-sm text-gray-500">IMEI: {product.imei} | Costo: ${product.costPrice}</div>
                          </div>
                          <Input
                            type="number"
                            placeholder="Precio venta"
                            defaultValue={product.costPrice}
                            className="w-24"
                            onKeyPress={(e) => {
                              if (e.key === 'Enter') {
                                const input = e.target as HTMLInputElement;
                                addProductToOrder(product, input.value);
                              }
                            }}
                            autoFocus={index === 0}
                          />
                        </div>
                      ))}
                    </div>
                  )}

                  {/* Productos en el pedido */}
                  {selectedItems.length > 0 && (
                    <div className="space-y-2">
                      <Label className="font-medium text-green-600">✓ En el Pedido ({selectedItems.length} productos)</Label>
                      {selectedItems.map((item) => (
                        <div key={item.productId} className="flex justify-between items-center p-2 bg-green-50 dark:bg-green-900/20 rounded">
                          <span>{item.product.model} - {item.product.storage}</span>
                          <span className="font-medium">${item.salePrice}</span>
                        </div>
                      ))}
                      <div className="text-right font-bold text-lg">
                        Total: ${totalAmount.toFixed(2)} USD
                      </div>
                      <Button
                        onClick={() => setCurrentStep('customer')}
                        className="w-full"
                        disabled={selectedItems.length === 0}
                      >
                        Continuar → Cliente (Enter)
                      </Button>
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>
          )}

          {/* PASO 2: Cliente */}
          {currentStep === 'customer' && (
            <Card>
              <CardContent className="p-6">
                <div className="space-y-4">
                  <Label className="text-lg font-medium">2. Información del Cliente</Label>
                  <Input
                    ref={customerRef}
                    placeholder="Nombre del cliente (Enter para continuar)"
                    value={customerInput}
                    onChange={(e) => setCustomerInput(e.target.value)}
                    onKeyPress={(e) => {
                      if (e.key === 'Enter' && customerInput.trim()) {
                        setCurrentStep('payment');
                      }
                    }}
                    autoFocus
                  />
                  <div className="flex space-x-2">
                    <Button
                      variant="outline"
                      onClick={() => setCurrentStep('scan')}
                    >
                      ← Volver
                    </Button>
                    <Button
                      onClick={() => setCurrentStep('payment')}
                      disabled={!customerInput.trim()}
                      className="flex-1"
                    >
                      Continuar → Pago (Enter)
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          {/* PASO 3: Pago */}
          {currentStep === 'payment' && (
            <Card>
              <CardContent className="p-6">
                <div className="space-y-4">
                  <Label className="text-lg font-medium">3. Métodos de Pago (Teclas 1-6)</Label>
                  
                  <div className="grid grid-cols-3 gap-2">
                    {availablePaymentMethods.map((method) => (
                      <Button
                        key={method.key}
                        variant={paymentMethods.includes(method.key) ? "default" : "outline"}
                        onClick={() => {
                          if (paymentMethods.includes(method.key)) {
                            setPaymentMethods(paymentMethods.filter(m => m !== method.key));
                            const newAmounts = {...paymentAmounts};
                            delete newAmounts[method.key];
                            setPaymentAmounts(newAmounts);
                          } else {
                            setPaymentMethods([...paymentMethods, method.key]);
                            setPaymentAmounts({
                              ...paymentAmounts,
                              [method.key]: ''
                            });
                          }
                        }}
                        className="text-sm"
                      >
                        {method.shortcut}. {method.label}
                      </Button>
                    ))}
                  </div>

                  {paymentMethods.length > 0 && (
                    <div className="space-y-3">
                      <Label className="font-medium">Montos por Método</Label>
                      {paymentMethods.map((methodKey, index) => {
                        const method = availablePaymentMethods.find(m => m.key === methodKey);
                        return (
                          <div key={methodKey} className="flex items-center space-x-2">
                            <Label className="w-32 text-sm">{method?.label}:</Label>
                            <Input
                              ref={index === 0 ? firstPaymentRef : undefined}
                              type="number"
                              placeholder="0.00"
                              value={paymentAmounts[methodKey] || ''}
                              onChange={(e) => {
                                setPaymentAmounts({
                                  ...paymentAmounts,
                                  [methodKey]: e.target.value
                                });
                              }}
                              className="flex-1"
                              autoFocus={index === 0}
                            />
                          </div>
                        );
                      })}
                    </div>
                  )}

                  <div>
                    <Label className="font-medium">Observaciones (Opcional)</Label>
                    <textarea
                      value={observations}
                      onChange={(e) => setObservations(e.target.value)}
                      placeholder="Notas del pedido..."
                      rows={2}
                      className="mt-1 w-full px-3 py-2 border rounded-md text-sm"
                    />
                  </div>

                  <div className="flex space-x-2">
                    <Button
                      variant="outline"
                      onClick={() => setCurrentStep('customer')}
                    >
                      ← Volver
                    </Button>
                    <Button
                      onClick={() => {
                        createOrderMutation.mutate({
                          customerName: customerInput,
                          paymentStatus
                        });
                      }}
                      disabled={
                        createOrderMutation.isPending || 
                        paymentMethods.length === 0 ||
                        paymentMethods.some(method => !paymentAmounts[method] || parseFloat(paymentAmounts[method]) <= 0)
                      }
                      className="flex-1"
                    >
                      {createOrderMutation.isPending ? "Creando..." : "Crear Pedido"}
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>
          )}
        </div>
      </div>
    </div>
  );
}