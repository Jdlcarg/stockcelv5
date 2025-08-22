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
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Trash2, Plus, ShoppingCart, CreditCard, Calculator } from "lucide-react";
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

interface AdvancedOrderModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export default function AdvancedOrderModal({ open, onOpenChange }: AdvancedOrderModalProps) {
  const { user } = useAuth();
  const { toast } = useToast();
  const queryClient = useQueryClient();
  
  const [orderData, setOrderData] = useState({
    customerName: "",
    customerPhone: "",
    customerEmail: "",
    customerId: "",
  });
  
  const [selectedItems, setSelectedItems] = useState<OrderItem[]>([]);
  const [selectedProductId, setSelectedProductId] = useState<string>("");
  const [salePrice, setSalePrice] = useState("");
  const [paymentMethod, setPaymentMethod] = useState("");
  const [showSwap, setShowSwap] = useState(false);
  const [currentItemIndex, setCurrentItemIndex] = useState<number>(-1);
  const [exchangeRates, setExchangeRates] = useState({
    efectivo_pesos: 1000,
    transferencia_pesos: 1000,
    transferencia_financiera: 1050,
  });

  const paymentMethods = [
    { value: "efectivo_dolar", label: "Efectivo Dólar" },
    { value: "efectivo_pesos", label: "Efectivo Pesos" },
    { value: "transferencia_pesos", label: "Transferencia Pesos" },
    { value: "transferencia_usdt", label: "Transferencia USDT" },
    { value: "transferencia_financiera", label: "Transferencia Financiera" },
  ];

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

  // Filtrar productos disponibles que no estén ya en el pedido
  const availableProducts = products?.filter((product: Product) => 
    product.status === 'disponible' && 
    !selectedItems.some(item => item.productId === product.id)
  ) || [];

  const createOrderMutation = useMutation({
    mutationFn: async (data: any) => {
      // Crear el pedido
      const orderNumber = `ORD-${Date.now()}`;
      const totalUsd = selectedItems.reduce((sum, item) => sum + parseFloat(item.amountUsd || item.salePrice), 0);
      
      const order = await apiRequest('POST', '/api/orders', {
        clientId: user?.clientId,
        orderNumber,
        customerName: data.customerName,
        customerPhone: data.customerPhone || null,
        customerEmail: data.customerEmail || null,
        customerId: data.customerId ? parseInt(data.customerId) : null,
        totalUsd: totalUsd.toFixed(2),
        status: 'pendiente',
        paymentStatus: 'pendiente',
        vendorId: user?.id,
      });

      // Agregar items al pedido con sus formas de pago individuales
      for (const item of selectedItems) {
        await apiRequest('POST', '/api/order-items', {
          orderId: order.id,
          productId: item.productId,
          priceUsd: item.salePrice,
          paymentMethod: item.paymentMethod,
          exchangeRate: item.exchangeRate || null,
          amountUsd: item.amountUsd || item.salePrice,
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
        description: "Pedido creado con formas de pago individuales",
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
    setOrderData({ customerName: "", customerPhone: "", customerEmail: "", customerId: "" });
    setSelectedItems([]);
    setSelectedProductId("");
    setSalePrice("");
    setPaymentMethod("");
    setShowSwap(false);
    setCurrentItemIndex(-1);
  };

  const handleAddProduct = () => {
    if (!selectedProductId || !salePrice || !paymentMethod) {
      toast({
        title: "Error",
        description: "Debes seleccionar un producto, ingresar el precio de venta y elegir forma de pago",
        variant: "destructive",
      });
      return;
    }

    const product = products?.find((p: Product) => p.id === parseInt(selectedProductId));
    if (!product) return;

    const newItem: OrderItem = {
      productId: product.id,
      product,
      salePrice: parseFloat(salePrice).toFixed(2),
      paymentMethod,
      amountUsd: parseFloat(salePrice).toFixed(2),
    };

    setSelectedItems([...selectedItems, newItem]);
    setSelectedProductId("");
    setSalePrice("");
    setPaymentMethod("");
  };

  const handleRemoveProduct = (productId: number) => {
    setSelectedItems(selectedItems.filter(item => item.productId !== productId));
  };

  const handlePaymentMethodChange = (index: number, newPaymentMethod: string) => {
    const updatedItems = [...selectedItems];
    updatedItems[index].paymentMethod = newPaymentMethod;
    
    // Calculate amount based on payment method
    const salePrice = parseFloat(updatedItems[index].salePrice);
    let amountUsd = salePrice;
    let exchangeRate = undefined;

    if (newPaymentMethod === "efectivo_pesos" || newPaymentMethod === "transferencia_pesos") {
      exchangeRate = exchangeRates[newPaymentMethod as keyof typeof exchangeRates]?.toString();
      amountUsd = salePrice; // Already in USD
    } else if (newPaymentMethod === "transferencia_financiera") {
      exchangeRate = exchangeRates.transferencia_financiera.toString();
      amountUsd = salePrice; // Will be updated via swap
    }

    updatedItems[index].exchangeRate = exchangeRate;
    updatedItems[index].amountUsd = amountUsd.toString();
    
    setSelectedItems(updatedItems);
  };

  const handleSwapComplete = (swapData: {
    fromCurrency: string;
    toCurrency: string;
    fromAmount: number;
    toAmount: number;
    exchangeRate: number;
  }) => {
    if (currentItemIndex >= 0) {
      const updatedItems = [...selectedItems];
      updatedItems[currentItemIndex].exchangeRate = swapData.exchangeRate.toString();
      updatedItems[currentItemIndex].amountUsd = swapData.toCurrency === "USD" 
        ? swapData.toAmount.toString() 
        : (swapData.toAmount / swapData.exchangeRate).toString();
      
      setSelectedItems(updatedItems);
      
      // Register the currency exchange
      const registerExchange = async () => {
        try {
          await apiRequest("POST", "/api/cash-register/currency-exchange", {
            clientId: user?.clientId,
            fromCurrency: swapData.fromCurrency,
            toCurrency: swapData.toCurrency,
            fromAmount: swapData.fromAmount,
            toAmount: swapData.toAmount,
            exchangeRate: swapData.exchangeRate,
            category: "conversion_moneda",
            userId: user?.id,
            notes: `Conversión para producto ${updatedItems[currentItemIndex].product.model}`,
          });
        } catch (error) {
          console.error("Error registering currency exchange:", error);
        }
      };

      registerExchange();
    }
    
    setShowSwap(false);
    setCurrentItemIndex(-1);
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

    createOrderMutation.mutate(orderData);
  };

  const totalAmount = selectedItems.reduce((sum, item) => sum + parseFloat(item.amountUsd || item.salePrice), 0);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[1000px] max-h-[85vh] overflow-y-auto pb-0">
        <DialogHeader>
          <DialogTitle>Crear Nuevo Pedido</DialogTitle>
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

          {/* Agregar productos */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Agregar Productos</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                <div>
                  <Label htmlFor="product">Producto</Label>
                  <Select value={selectedProductId} onValueChange={setSelectedProductId}>
                    <SelectTrigger>
                      <SelectValue placeholder="Selecciona un producto" />
                    </SelectTrigger>
                    <SelectContent>
                      {availableProducts.map((product: Product) => (
                        <SelectItem key={product.id} value={product.id.toString()}>
                          {product.model} - {product.storage} - {product.color} 
                          <span className="text-sm text-gray-500 ml-2">
                            (Costo: ${product.costPrice})
                          </span>
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                
                <div>
                  <Label htmlFor="salePrice">Precio de Venta (USD)</Label>
                  <Input
                    id="salePrice"
                    type="number"
                    step="0.01"
                    value={salePrice}
                    onChange={(e) => setSalePrice(e.target.value)}
                    placeholder="0.00"
                  />
                </div>

                <div>
                  <Label htmlFor="paymentMethod">Forma de Pago</Label>
                  <Select value={paymentMethod} onValueChange={setPaymentMethod}>
                    <SelectTrigger>
                      <SelectValue placeholder="Seleccionar" />
                    </SelectTrigger>
                    <SelectContent>
                      {paymentMethods.map((method) => (
                        <SelectItem key={method.value} value={method.value}>
                          {method.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                
                <div className="flex items-end">
                  <Button
                    type="button"
                    onClick={handleAddProduct}
                    className="w-full"
                    disabled={!selectedProductId || !salePrice || !paymentMethod}
                  >
                    <Plus className="mr-2 h-4 w-4" />
                    Agregar
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Lista de productos seleccionados */}
          {selectedItems.length > 0 && (
            <Card>
              <CardHeader>
                <CardTitle className="text-lg flex items-center justify-between">
                  <div className="flex items-center">
                    <ShoppingCart className="mr-2 h-5 w-5" />
                    Productos en el Pedido
                  </div>
                  <div className="text-right">
                    <span className="text-sm text-gray-600">Total: </span>
                    <span className="text-lg font-bold text-green-600">${totalAmount.toFixed(2)}</span>
                  </div>
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  {selectedItems.map((item, index) => (
                    <div key={item.productId} className="border rounded-lg p-4 bg-gray-50">
                      <div className="flex items-start justify-between">
                        <div className="flex-1">
                          <div className="font-medium text-lg">
                            {item.product.model} - {item.product.storage} - {item.product.color}
                          </div>
                          <div className="text-sm text-gray-500 mb-2">
                            IMEI: {item.product.imei} | Costo: ${item.product.costPrice}
                          </div>
                          <div className="text-sm font-medium text-green-600">
                            Precio de Venta: ${item.salePrice}
                          </div>
                        </div>
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
                      
                      <div className="mt-3 flex items-center gap-4">
                        <div className="flex-1">
                          <Label className="text-sm font-medium">Forma de Pago</Label>
                          <Select 
                            value={item.paymentMethod} 
                            onValueChange={(value) => handlePaymentMethodChange(index, value)}
                          >
                            <SelectTrigger className="mt-1">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              {paymentMethods.map((method) => (
                                <SelectItem key={method.value} value={method.value}>
                                  {method.label}
                                </SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                        
                        {(item.paymentMethod === "transferencia_financiera" || 
                          item.paymentMethod === "efectivo_pesos" || 
                          item.paymentMethod === "transferencia_pesos") && (
                          <div className="flex items-center gap-2">
                            {item.paymentMethod === "transferencia_financiera" && (
                              <Button
                                type="button"
                                variant="outline"
                                size="sm"
                                onClick={() => {
                                  setCurrentItemIndex(index);
                                  setShowSwap(true);
                                }}
                              >
                                <Calculator className="h-4 w-4 mr-1" />
                                Cotizar
                              </Button>
                            )}
                            <div className="text-sm text-gray-600">
                              {item.paymentMethod === "transferencia_financiera" && item.exchangeRate && (
                                <>Tasa: {item.exchangeRate} | USD: ${item.amountUsd}</>
                              )}
                              {(item.paymentMethod === "efectivo_pesos" || item.paymentMethod === "transferencia_pesos") && (
                                <>Tasa: {exchangeRates[item.paymentMethod as keyof typeof exchangeRates]} | USD: ${(parseFloat(item.salePrice) / exchangeRates[item.paymentMethod as keyof typeof exchangeRates]).toFixed(2)}</>
                              )}
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          )}

          {/* Botones de acción */}
          <div className="flex justify-end gap-2 pt-4 border-t bg-white sticky bottom-0 mt-4">
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
              Cancelar
            </Button>
            <Button
              type="submit"
              disabled={createOrderMutation.isPending || selectedItems.length === 0}
            >
              {createOrderMutation.isPending ? "Creando..." : "Crear Pedido"}
            </Button>
          </div>
        </form>

        {/* Currency Swap Modal */}
        {showSwap && (
          <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
            <div className="bg-white rounded-lg p-6 max-w-md w-full">
              <div className="flex items-center justify-between mb-4">
                <h2 className="text-lg font-semibold">Conversión de Moneda</h2>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => setShowSwap(false)}
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