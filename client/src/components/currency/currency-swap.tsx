import { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { ArrowUpDown, DollarSign, Calculator } from "lucide-react";
import { toast } from "@/hooks/use-toast";

interface CurrencySwapProps {
  onSwapComplete: (swapData: {
    fromCurrency: string;
    toCurrency: string;
    fromAmount: number;
    toAmount: number;
    exchangeRate: number;
  }) => void;
}

export default function CurrencySwap({ onSwapComplete }: CurrencySwapProps) {
  const [fromCurrency, setFromCurrency] = useState("USD");
  const [toCurrency, setToCurrency] = useState("ARS");
  const [fromAmount, setFromAmount] = useState("");
  const [toAmount, setToAmount] = useState("");
  const [exchangeRate, setExchangeRate] = useState("1000");
  const [isCalculating, setIsCalculating] = useState(false);

  const currencies = [
    { code: "USD", name: "USD", symbol: "$" },
    { code: "ARS", name: "ARS", symbol: "$" },
    { code: "USDT", name: "USDT", symbol: "₮" },
  ];

  const handleSwapCurrencies = () => {
    const tempCurrency = fromCurrency;
    const tempAmount = fromAmount;
    
    setFromCurrency(toCurrency);
    setToCurrency(tempCurrency);
    setFromAmount(toAmount);
    setToAmount(tempAmount);
    
    // Invert exchange rate
    if (exchangeRate) {
      const rate = parseFloat(exchangeRate);
      if (rate !== 0) {
        setExchangeRate((1 / rate).toFixed(4));
      }
    }
  };

  const calculateToAmount = () => {
    if (!fromAmount || !exchangeRate) return;
    
    const from = parseFloat(fromAmount);
    const rate = parseFloat(exchangeRate);
    
    if (isNaN(from) || isNaN(rate)) return;
    
    const to = from * rate;
    setToAmount(to.toFixed(2));
  };

  const calculateFromAmount = () => {
    if (!toAmount || !exchangeRate) return;
    
    const to = parseFloat(toAmount);
    const rate = parseFloat(exchangeRate);
    
    if (isNaN(to) || isNaN(rate) || rate === 0) return;
    
    const from = to / rate;
    setFromAmount(from.toFixed(2));
  };

  const handleFromAmountChange = (value: string) => {
    setFromAmount(value);
    if (value && exchangeRate) {
      const from = parseFloat(value);
      const rate = parseFloat(exchangeRate);
      if (!isNaN(from) && !isNaN(rate)) {
        const to = from * rate;
        setToAmount(to.toFixed(2));
      }
    }
  };

  const handleToAmountChange = (value: string) => {
    setToAmount(value);
    if (value && exchangeRate) {
      const to = parseFloat(value);
      const rate = parseFloat(exchangeRate);
      if (!isNaN(to) && !isNaN(rate) && rate !== 0) {
        const from = to / rate;
        setFromAmount(from.toFixed(2));
      }
    }
  };

  const handleExchangeRateChange = (value: string) => {
    setExchangeRate(value);
    if (fromAmount && value) {
      const from = parseFloat(fromAmount);
      const rate = parseFloat(value);
      if (!isNaN(from) && !isNaN(rate)) {
        const to = from * rate;
        setToAmount(to.toFixed(2));
      }
    }
  };

  const handleConfirmSwap = () => {
    if (!fromAmount || !toAmount || !exchangeRate) {
      toast({
        title: "Campos incompletos",
        description: "Por favor completa todos los campos",
        variant: "destructive",
      });
      return;
    }

    const swapData = {
      fromCurrency,
      toCurrency,
      fromAmount: parseFloat(fromAmount),
      toAmount: parseFloat(toAmount),
      exchangeRate: parseFloat(exchangeRate),
    };

    onSwapComplete(swapData);
  };

  const getMaxAmount = () => {
    // This would typically come from the cash register or wallet balance
    return 100000;
  };

  const getCurrencySymbol = (code: string) => {
    const currency = currencies.find(c => c.code === code);
    return currency?.symbol || "$";
  };

  return (
    <Card className="w-full max-w-sm mx-auto">
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-base">
          <Calculator className="h-4 w-4" />
          Conversión de Moneda
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {/* From Currency */}
        <div className="space-y-1">
          <Label htmlFor="from-currency" className="text-sm">Moneda de origen</Label>
          <Select value={fromCurrency} onValueChange={setFromCurrency}>
            <SelectTrigger className="h-9">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {currencies.map((currency) => (
                <SelectItem key={currency.code} value={currency.code}>
                  {currency.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {/* From Amount */}
        <div className="space-y-1">
          <Label htmlFor="from-amount" className="text-sm">Monto a convertir</Label>
          <div className="relative">
            <Input
              id="from-amount"
              type="number"
              placeholder="0.00"
              value={fromAmount}
              onChange={(e) => handleFromAmountChange(e.target.value)}
              className="pl-8 h-9"
              min="0"
              step="0.01"
            />
            <span className="absolute left-2 top-1/2 transform -translate-y-1/2 text-gray-500 text-sm">
              {getCurrencySymbol(fromCurrency)}
            </span>
          </div>
        </div>

        {/* Swap Button */}
        <div className="flex justify-center py-1">
          <Button
            variant="outline"
            size="sm"
            onClick={handleSwapCurrencies}
            className="rounded-full p-1.5 h-8 w-8"
          >
            <ArrowUpDown className="h-3 w-3" />
          </Button>
        </div>

        {/* To Currency */}
        <div className="space-y-1">
          <Label htmlFor="to-currency" className="text-sm">Moneda de destino</Label>
          <Select value={toCurrency} onValueChange={setToCurrency}>
            <SelectTrigger className="h-9">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {currencies.map((currency) => (
                <SelectItem key={currency.code} value={currency.code}>
                  {currency.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {/* To Amount */}
        <div className="space-y-1">
          <Label htmlFor="to-amount" className="text-sm">Monto a recibir</Label>
          <div className="relative">
            <Input
              id="to-amount"
              type="number"
              placeholder="0.00"
              value={toAmount}
              onChange={(e) => handleToAmountChange(e.target.value)}
              className="pl-8 h-9"
              min="0"
              step="0.01"
            />
            <span className="absolute left-2 top-1/2 transform -translate-y-1/2 text-gray-500 text-sm">
              {getCurrencySymbol(toCurrency)}
            </span>
          </div>
        </div>

        {/* Exchange Rate */}
        <div className="space-y-1">
          <Label htmlFor="exchange-rate" className="text-sm">Cotización ({fromCurrency} → {toCurrency})</Label>
          <div className="relative">
            <Input
              id="exchange-rate"
              type="number"
              placeholder="1000"
              value={exchangeRate}
              onChange={(e) => handleExchangeRateChange(e.target.value)}
              className="pl-8 h-9"
              min="0"
              step="0.0001"
            />
            <DollarSign className="absolute left-2 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-500" />
          </div>
        </div>

        {/* Preview */}
        {fromAmount && toAmount && (
          <div className="bg-gray-50 dark:bg-gray-800 p-2 rounded">
            <div className="text-xs font-medium text-center">
              {getCurrencySymbol(fromCurrency)}{parseFloat(fromAmount).toLocaleString()} {fromCurrency} → {getCurrencySymbol(toCurrency)}{parseFloat(toAmount).toLocaleString()} {toCurrency}
            </div>
          </div>
        )}

        {/* Confirm Button */}
        <Button 
          onClick={handleConfirmSwap}
          className="w-full h-9"
          disabled={!fromAmount || !toAmount || !exchangeRate}
        >
          Confirmar Conversión
        </Button>
      </CardContent>
    </Card>
  );
}