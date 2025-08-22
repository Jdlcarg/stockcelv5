import { useState, useEffect } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useAuth } from "@/lib/auth";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { PlusIcon, X, Calculator, DollarSign, ArrowUpDown, Settings } from "lucide-react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { apiRequest } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import type { Order } from "@shared/schema";
import CurrencySwap from "@/components/currency/currency-swap";
import ExchangeRateEditor from "./exchange-rate-editor";

interface Payment {
  id: string;
  paymentMethod: string;
  amount: string;
  exchangeRate?: string;
  amountUsd: string;
}

interface MixedPaymentsModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  order: Order;
  onPaymentComplete: () => void;
}

const paymentSchema = z.object({
  paymentMethod: z.string().min(1, "Forma de pago requerida"),
  amount: z.string().min(1, "Monto requerido"),
  exchangeRate: z.string().optional(),
});

export default function MixedPaymentsModal({ 
  open, 
  onOpenChange, 
  order, 
  onPaymentComplete 
}: MixedPaymentsModalProps) {
  const { user } = useAuth();
  const { toast } = useToast();
  const queryClient = useQueryClient();
  
  const [payments, setPayments] = useState<Payment[]>([]);
  const [currentPayment, setCurrentPayment] = useState<Payment>({
    id: "",
    paymentMethod: "",
    amount: "",
    exchangeRate: "",
    amountUsd: "",
  });
  const [totalPaid, setTotalPaid] = useState(0);
  const [remainingAmount, setRemainingAmount] = useState(0);
  const [showSwap, setShowSwap] = useState(false);

  const form = useForm({
    resolver: zodResolver(paymentSchema),
    defaultValues: {
      paymentMethod: "",
      amount: "",
      exchangeRate: "",
    },
  });

  const paymentMethods = [
    { value: "efectivo_dolar", label: "Efectivo Dólar", needsRate: false },
    { value: "efectivo_pesos", label: "Efectivo Pesos", needsRate: true },
    { value: "transferencia_pesos", label: "Transferencia Pesos", needsRate: true },
    { value: "transferencia_usdt", label: "Transferencia USDT", needsRate: false },
    { value: "transferencia_financiera", label: "Transferencia Financiera", needsRate: true, hasSwap: true },
  ];

  const [exchangeRates, setExchangeRates] = useState({
    efectivo_dolar: 1,
    efectivo_pesos: 1000,
    transferencia_pesos: 1000,
    transferencia_usdt: 1,
    transferencia_financiera: 1050, // Example rate with margin
  });

  const [showExchangeEditor, setShowExchangeEditor] = useState(false);
  const [tempExchangeRate, setTempExchangeRate] = useState("");



  useEffect(() => {
    if (open) {
      const orderTotal = parseFloat(order.totalUsd);
      setRemainingAmount(orderTotal);
      setTotalPaid(0);
      setPayments([]);
    }
  }, [open, order]);

  useEffect(() => {
    const total = payments.reduce((sum, payment) => sum + parseFloat(payment.amountUsd), 0);
    setTotalPaid(total);
    setRemainingAmount(parseFloat(order.totalUsd) - total);
  }, [payments, order]);

  const calculateUsdAmount = (amount: string, method: string, rate?: string) => {
    if (!amount || !method) return "";
    
    const amountNum = parseFloat(amount);
    const rateNum = rate ? parseFloat(rate) : exchangeRates[method as keyof typeof exchangeRates] || 1;
    
    if (method === "efectivo_dolar" || method === "transferencia_usdt") {
      return amountNum.toFixed(2);
    } else {
      return (amountNum / rateNum).toFixed(2);
    }
  };

  const handlePaymentMethodChange = (method: string) => {
    const methodConfig = paymentMethods.find(m => m.value === method);
    const defaultRate = exchangeRates[method as keyof typeof exchangeRates]?.toString() || "";
    
    setCurrentPayment(prev => ({
      ...prev,
      paymentMethod: method,
      exchangeRate: methodConfig?.needsRate ? defaultRate : "",
      amountUsd: calculateUsdAmount(prev.amount, method, defaultRate),
    }));
  };

  const handleEditExchangeRate = (method: string) => {
    setTempExchangeRate(exchangeRates[method as keyof typeof exchangeRates]?.toString() || "");
    setShowExchangeEditor(true);
  };

  const updateExchangeRate = (method: string, newRate: string) => {
    const rateNum = parseFloat(newRate);
    if (isNaN(rateNum) || rateNum <= 0) {
      toast({ title: "Tasa de cambio inválida", variant: "destructive" });
      return;
    }

    setExchangeRates(prev => ({
      ...prev,
      [method]: rateNum,
    }));

    // Update current payment if it uses this method
    if (currentPayment.paymentMethod === method) {
      setCurrentPayment(prev => ({
        ...prev,
        exchangeRate: newRate,
        amountUsd: calculateUsdAmount(prev.amount, method, newRate),
      }));
    }

    setShowExchangeEditor(false);
    toast({ title: "Tasa de cambio actualizada" });
  };

  const handleAmountChange = (amount: string) => {
    setCurrentPayment(prev => ({
      ...prev,
      amount,
      amountUsd: calculateUsdAmount(amount, prev.paymentMethod, prev.exchangeRate),
    }));
  };

  const handleExchangeRateChange = (rate: string) => {
    setCurrentPayment(prev => ({
      ...prev,
      exchangeRate: rate,
      amountUsd: calculateUsdAmount(prev.amount, prev.paymentMethod, rate),
    }));
  };

  const addPayment = () => {
    if (!currentPayment.paymentMethod || !currentPayment.amount) {
      toast({ title: "Complete todos los campos", variant: "destructive" });
      return;
    }

    const newPayment: Payment = {
      ...currentPayment,
      id: Date.now().toString(),
    };

    setPayments(prev => [...prev, newPayment]);
    setCurrentPayment({
      id: "",
      paymentMethod: "",
      amount: "",
      exchangeRate: "",
      amountUsd: "",
    });
    form.reset();
  };

  const removePayment = (id: string) => {
    setPayments(prev => prev.filter(p => p.id !== id));
  };

  const handleSwapComplete = (swapData: {
    fromCurrency: string;
    toCurrency: string;
    fromAmount: number;
    toAmount: number;
    exchangeRate: number;
  }) => {
    // Register the currency exchange in the cash register
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
        });
        
        toast({
          title: "Conversión registrada",
          description: `${swapData.fromAmount} ${swapData.fromCurrency} → ${swapData.toAmount} ${swapData.toCurrency}`,
        });
      } catch (error) {
        toast({
          title: "Error al registrar conversión",
          variant: "destructive",
        });
      }
    };

    registerExchange();
    
    // Update current payment with swap data
    setCurrentPayment(prev => ({
      ...prev,
      exchangeRate: swapData.exchangeRate.toString(),
      amountUsd: swapData.toCurrency === "USD" ? swapData.toAmount.toString() : (swapData.toAmount / swapData.exchangeRate).toString(),
    }));
    
    setShowSwap(false);
  };

  const processPaymentsMutation = useMutation({
    mutationFn: async () => {
      // Process each payment
      for (const payment of payments) {
        await apiRequest("POST", "/api/payments", {
          clientId: user?.clientId,
          orderId: order.id,
          paymentMethod: payment.paymentMethod,
          amount: payment.amount,
          exchangeRate: payment.exchangeRate || null,
          amountUsd: payment.amountUsd,
        });
      }

      // Update order payment status
      const newStatus = Math.abs(remainingAmount) < 0.01 ? "pagado" : "parcial";
      await apiRequest("PATCH", `/api/orders/${order.id}`, {
        paymentStatus: newStatus,
      });

      return { success: true };
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/orders"] });
      onPaymentComplete();
      onOpenChange(false);
      toast({ title: "Pagos procesados exitosamente" });
    },
    onError: () => {
      toast({ title: "Error al procesar pagos", variant: "destructive" });
    },
  });

  const canProcess = payments.length > 0 && Math.abs(remainingAmount) < 0.01;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-4xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Calculator className="h-5 w-5" />
            Pagos Mixtos - Pedido #{order.orderNumber}
          </DialogTitle>
        </DialogHeader>

        <div className="space-y-6">
          {/* Payment Summary */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Resumen del Pago</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-3 gap-4">
                <div>
                  <Label className="text-sm font-medium">Total del Pedido</Label>
                  <div className="text-2xl font-bold text-blue-600">
                    ${parseFloat(order.totalUsd).toFixed(2)}
                  </div>
                </div>
                <div>
                  <Label className="text-sm font-medium">Total Pagado</Label>
                  <div className="text-2xl font-bold text-green-600">
                    ${totalPaid.toFixed(2)}
                  </div>
                </div>
                <div>
                  <Label className="text-sm font-medium">Restante</Label>
                  <div className={`text-2xl font-bold ${remainingAmount > 0 ? 'text-red-600' : 'text-green-600'}`}>
                    ${remainingAmount.toFixed(2)}
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Exchange Rate Configuration */}
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle className="text-lg">Tasas de Cambio</CardTitle>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setShowExchangeEditor(true)}
                >
                  <Settings className="h-4 w-4 mr-2" />
                  Configurar
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-2 md:grid-cols-5 gap-4 text-sm">
                {paymentMethods.map(method => (
                  <div key={method.value} className="text-center">
                    <div className="font-medium">{method.label}</div>
                    <div className="text-gray-600 font-mono">
                      {method.needsRate ? 
                        `1 USD = ${exchangeRates[method.value as keyof typeof exchangeRates] || 1}` : 
                        "1:1"
                      }
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>

          {/* Add Payment Form */}
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Agregar Pago</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <Label>Forma de Pago</Label>
                  <Select value={currentPayment.paymentMethod} onValueChange={handlePaymentMethodChange}>
                    <SelectTrigger>
                      <SelectValue placeholder="Seleccionar forma de pago" />
                    </SelectTrigger>
                    <SelectContent>
                      {paymentMethods.map(method => (
                        <SelectItem key={method.value} value={method.value}>
                          {method.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                <div>
                  <Label>Monto</Label>
                  <Input
                    type="number"
                    step="0.01"
                    value={currentPayment.amount}
                    onChange={(e) => handleAmountChange(e.target.value)}
                    placeholder="0.00"
                  />
                </div>
              </div>

              {currentPayment.paymentMethod && paymentMethods.find(m => m.value === currentPayment.paymentMethod)?.needsRate && (
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <Label>Cotización</Label>
                    <div className="flex gap-2">
                      <Input
                        type="number"
                        step="0.0001"
                        value={currentPayment.exchangeRate}
                        onChange={(e) => handleExchangeRateChange(e.target.value)}
                        placeholder="1.0000"
                      />
                      {currentPayment.paymentMethod === "transferencia_financiera" && (
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => setShowSwap(true)}
                          className="px-3"
                        >
                          <ArrowUpDown className="h-4 w-4" />
                        </Button>
                      )}
                    </div>
                  </div>
                  <div>
                    <Label>Equivalente en USD</Label>
                    <Input
                      type="number"
                      value={currentPayment.amountUsd}
                      readOnly
                      className="bg-gray-50"
                    />
                  </div>
                </div>
              )}

              <Button onClick={addPayment} disabled={!currentPayment.paymentMethod || !currentPayment.amount}>
                <PlusIcon className="h-4 w-4 mr-2" />
                Agregar Pago
              </Button>
            </CardContent>
          </Card>

          {/* Payments List */}
          {payments.length > 0 && (
            <Card>
              <CardHeader>
                <CardTitle className="text-lg">Pagos Agregados</CardTitle>
              </CardHeader>
              <CardContent>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Forma de Pago</TableHead>
                      <TableHead>Monto</TableHead>
                      <TableHead>Cotización</TableHead>
                      <TableHead>USD</TableHead>
                      <TableHead>Acciones</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {payments.map((payment) => (
                      <TableRow key={payment.id}>
                        <TableCell>
                          <Badge variant="outline">
                            {paymentMethods.find(m => m.value === payment.paymentMethod)?.label}
                          </Badge>
                        </TableCell>
                        <TableCell className="font-mono">
                          {payment.amount}
                        </TableCell>
                        <TableCell className="font-mono">
                          {payment.exchangeRate || "1.0000"}
                        </TableCell>
                        <TableCell className="font-mono font-bold">
                          ${payment.amountUsd}
                        </TableCell>
                        <TableCell>
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => removePayment(payment.id)}
                          >
                            <X className="h-4 w-4" />
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          )}

          {/* Process Button */}
          <div className="flex justify-end gap-4">
            <Button variant="outline" onClick={() => onOpenChange(false)}>
              Cancelar
            </Button>
            <Button
              onClick={() => processPaymentsMutation.mutate()}
              disabled={!canProcess || processPaymentsMutation.isPending}
            >
              <DollarSign className="h-4 w-4 mr-2" />
              {processPaymentsMutation.isPending ? "Procesando..." : "Procesar Pagos"}
            </Button>
          </div>
        </div>
      </DialogContent>
      
      {/* Currency Swap Modal */}
      <Dialog open={showSwap} onOpenChange={setShowSwap}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>Conversión de Moneda - Financiera</DialogTitle>
          </DialogHeader>
          <CurrencySwap onSwapComplete={handleSwapComplete} />
        </DialogContent>
      </Dialog>

      {/* Exchange Rate Editor */}
      <ExchangeRateEditor
        open={showExchangeEditor}
        onOpenChange={setShowExchangeEditor}
        exchangeRates={exchangeRates}
        onUpdateRates={setExchangeRates}
      />
    </Dialog>
  );
}