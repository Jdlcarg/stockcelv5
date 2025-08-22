import { useState, useEffect } from "react";
import { useLocation, useRoute } from "wouter";
import { useAuth } from "@/lib/auth";
import { useQuery } from "@tanstack/react-query";
import { Button } from "@/components/ui/button";
import { ArrowLeft, Printer } from "lucide-react";

export default function InvoiceNew() {
  const [, params] = useRoute("/invoice/:orderId");
  const orderId = params?.orderId;
  const [, setLocation] = useLocation();
  const { isAuthenticated, user } = useAuth();

  useEffect(() => {
    if (!isAuthenticated) {
      setLocation("/");
    }
  }, [isAuthenticated, setLocation]);

  // Obtener datos del pedido
  const { data: order, isLoading: orderLoading } = useQuery({
    queryKey: ['/api/orders', orderId],
    queryFn: async () => {
      const response = await fetch(`/api/orders/${orderId}?cache=${Date.now()}`);
      if (!response.ok) throw new Error('Failed to fetch order');
      return response.json();
    },
    enabled: !!orderId,
    cacheTime: 0, // No cache
    staleTime: 0, // Always consider stale
    refetchOnWindowFocus: true,
    refetchOnMount: true,
  });

  // Obtener items del pedido
  const { data: orderItems, isLoading: itemsLoading } = useQuery({
    queryKey: ['/api/order-items', orderId],
    queryFn: async () => {
      const response = await fetch(`/api/order-items?orderId=${orderId}`);
      if (!response.ok) throw new Error('Failed to fetch order items');
      return response.json();
    },
    enabled: !!orderId,
  });

  // Obtener configuración de la empresa
  const { data: companyConfig, isLoading: companyLoading } = useQuery({
    queryKey: ['/api/company-configuration', user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/company-configuration?clientId=${user?.clientId}`);
      if (!response.ok) {
        if (response.status === 404) return null;
        throw new Error('Failed to fetch company configuration');
      }
      const data = await response.json();
      console.log('🏢 Company Config en factura:', data);
      console.log('🖼️ Logo URL disponible:', data?.logoUrl);
      console.log('🔍 Verificando logo URL:', data?.logoUrl ? 'SI' : 'NO');
      return data;
    },
    enabled: !!user?.clientId,
  });

  // Obtener pagos del pedido
  const { data: payments, isLoading: paymentsLoading } = useQuery({
    queryKey: ['/api/payments', orderId],
    queryFn: async () => {
      const response = await fetch(`/api/payments?orderId=${orderId}&cache=${Date.now()}`);
      if (!response.ok) throw new Error('Failed to fetch payments');
      return response.json();
    },
    enabled: !!orderId,
    cacheTime: 0, // No cache
    staleTime: 0, // Always consider stale
    refetchOnWindowFocus: true,
    refetchOnMount: true,
    refetchInterval: 2000, // Refetch every 2 seconds for real-time updates
  });

  const handlePrint = () => {
    window.print();
  };

  const handleBack = () => {
    setLocation("/orders");
  };

  if (orderLoading || itemsLoading || companyLoading || paymentsLoading) {
    return (
      <div className="min-h-screen bg-gray-50 dark:bg-gray-900 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <p className="text-gray-600 dark:text-gray-400">Cargando factura...</p>
        </div>
      </div>
    );
  }

  if (!order) {
    return (
      <div className="min-h-screen bg-gray-50 dark:bg-gray-900 flex items-center justify-center">
        <div className="text-center">
          <p className="text-red-600 dark:text-red-400 mb-4">Pedido no encontrado</p>
          <Button onClick={handleBack} variant="outline">
            <ArrowLeft className="w-4 h-4 mr-2" />
            Volver a Pedidos
          </Button>
        </div>
      </div>
    );
  }

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString('es-AR', {
      day: '2-digit',
      month: '2-digit', 
      year: 'numeric'
    });
  };

  const formatCurrency = (amount: string | number) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 2
    }).format(typeof amount === 'string' ? parseFloat(amount) : amount);
  };

  const formatPaymentMethod = (method: string) => {
    const methodTranslations: { [key: string]: string } = {
      'efectivo_ars': 'Efectivo ARS',
      'efectivo_usd': 'Efectivo USD',
      'transferencia_usdt': 'Transferencia USDT',
      'transferencia_usd': 'Transferencia USD',
      'tarjeta_credito': 'Tarjeta de Crédito',
      'tarjeta_debito': 'Tarjeta de Débito',
      'financiera': 'Financiera'
    };
    return methodTranslations[method] || method.replace('_', ' ').toUpperCase();
  };

  const getSubtotal = () => {
    if (!orderItems || orderItems.length === 0) return 0;
    return orderItems.reduce((sum: number, item: any) => {
      return sum + (parseFloat(item.priceUsd) * item.quantity);
    }, 0);
  };

  const subtotal = getSubtotal();

  // Calcular total pagado
  const totalPaid = payments?.reduce((total: number, payment: any) => {
    return total + parseFloat(payment.amountUsd || '0');
  }, 0) || 0;

  // Calcular deuda restante
  const remainingDebt = subtotal - totalPaid;

  // Determinar estado del pago
  const getPaymentStatus = () => {
    console.log('Payment Status Debug:', { totalPaid, subtotal, remainingDebt });
    
    if (totalPaid === 0) return { 
      status: 'no_pagado', 
      label: 'No Pagado', 
      color: 'text-red-600', 
      bgColor: 'bg-red-50 border-red-200',
      icon: '❌'
    };
    if (remainingDebt <= 0.01) return { // Tolerancia para errores de redondeo
      status: 'completo', 
      label: 'Pagado Completo', 
      color: 'text-green-600', 
      bgColor: 'bg-green-50 border-green-200',
      icon: '✅'
    };
    return { 
      status: 'parcial', 
      label: 'Pago Parcial', 
      color: 'text-yellow-600', 
      bgColor: 'bg-yellow-50 border-yellow-200',
      icon: '⚠️'
    };
  };

  const paymentStatus = getPaymentStatus();

  return (
    <div className="min-h-screen bg-gray-100 dark:bg-gray-900">
      {/* Print styles */}
      <style jsx global>{`
        @media print {
          .no-print { display: none !important; }
          .print-only { display: block !important; }
          body { margin: 0; background: white !important; }
          .invoice-container { margin: 0; padding: 20px; box-shadow: none; border: none; }
          .page-break { page-break-before: always; }
          .invoice-header { background: #1e3a8a !important; }
          * { -webkit-print-color-adjust: exact !important; color-adjust: exact !important; }
        }
      `}</style>

      {/* Header con botones - no imprimir */}
      <div className="no-print bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 px-6 py-4 flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <Button onClick={handleBack} variant="outline">
            <ArrowLeft className="w-4 h-4 mr-2" />
            Volver
          </Button>
          <h1 className="text-xl font-semibold text-gray-900 dark:text-white">
            Factura #{order.orderNumber}
          </h1>
        </div>
        <Button onClick={handlePrint} className="bg-blue-600 hover:bg-blue-700">
          <Printer className="w-4 h-4 mr-2" />
          Imprimir
        </Button>
      </div>

      {/* Contenido de la factura */}
      <div className="invoice-container max-w-4xl mx-auto bg-white dark:bg-gray-800 shadow-2xl border border-gray-200 dark:border-gray-700 my-8 overflow-hidden">
        {/* Header con diseño azul profesional mejorado */}
        <div className="invoice-header bg-gradient-to-br from-blue-800 via-blue-900 to-indigo-900 text-white p-8 relative overflow-hidden">
          {/* Elementos decorativos de fondo */}
          <div className="absolute top-0 right-0 w-64 h-64 bg-white bg-opacity-5 rounded-full -translate-y-32 translate-x-32"></div>
          <div className="absolute bottom-0 left-0 w-32 h-32 bg-white bg-opacity-5 rounded-full translate-y-16 -translate-x-16"></div>
          <div className="absolute top-10 left-1/3 w-2 h-2 bg-blue-300 rounded-full animate-pulse"></div>
          <div className="absolute bottom-20 right-1/4 w-3 h-3 bg-cyan-300 rounded-full opacity-60"></div>
          
          <div className="relative z-10 flex justify-between items-start">
            <div className="flex-1">
              <div className="flex items-center mb-6">
                <div className="w-16 h-16 bg-gradient-to-br from-white/20 to-white/10 backdrop-blur-sm rounded-xl flex items-center justify-center mr-6 shadow-lg border border-white/20">
                  {companyConfig && companyConfig.logoUrl && companyConfig.logoUrl.trim() !== '' ? (
                    <img 
                      src={companyConfig.logoUrl} 
                      alt="Logo de la empresa" 
                      className="w-14 h-14 object-contain rounded-lg bg-white/10 p-1"
                      onLoad={() => console.log('✅ Logo cargado exitosamente:', companyConfig.logoUrl)}
                      onError={(e) => {
                        console.log('❌ Error cargando logo:', companyConfig.logoUrl);
                        console.log('❌ Error details:', e);
                        // Ocultar imagen con error y mostrar fallback
                        e.currentTarget.style.display = 'none';
                        const parent = e.currentTarget.parentElement;
                        if (parent) {
                          parent.innerHTML = '<span class="text-3xl">📱</span>';
                        }
                      }}
                    />
                  ) : (
                    <span className="text-3xl">📱</span>
                  )}
                </div>
                <div>
                  <h1 className="text-3xl font-bold mb-1 tracking-wide">
                    {companyConfig?.companyName || 'StockCel'}
                  </h1>
                  <p className="text-blue-100 text-sm font-medium tracking-wider uppercase">
                    Gestión de Stock Móviles
                  </p>
                  <div className="w-24 h-1 bg-gradient-to-r from-blue-300 to-cyan-300 rounded-full mt-2"></div>
                </div>
              </div>
              
              <div className="bg-gradient-to-r from-blue-700/80 to-blue-600/80 backdrop-blur-sm p-6 rounded-xl border border-white/10 shadow-lg">
                <h3 className="font-semibold mb-3 text-blue-100">Facturado a:</h3>
                <p className="font-bold text-xl mb-2">{order.customerName}</p>
                {companyConfig && (
                  <div className="text-sm text-blue-100 space-y-1">
                    <p className="flex items-center">
                      <span className="w-4 h-4 mr-2">📍</span>
                      {companyConfig.address}
                    </p>
                    <p className="flex items-center">
                      <span className="w-4 h-4 mr-2">📞</span>
                      {companyConfig.phone}
                    </p>
                  </div>
                )}
              </div>
            </div>
            
            <div className="text-right">
              <h1 className="text-4xl font-bold mb-4 bg-gradient-to-r from-white to-blue-100 bg-clip-text text-transparent">
                FACTURA
              </h1>
              <div className="bg-white text-gray-800 p-6 rounded-xl shadow-lg border border-gray-200">
                <div className="mb-4">
                  <span className="text-sm font-medium text-gray-600">Factura N°</span>
                  <p className="font-bold text-lg text-blue-800">{order.orderNumber.replace('ORD-', '')}</p>
                </div>
                <div className="border-t pt-3">
                  <span className="text-sm font-medium text-gray-600">Fecha</span>
                  <p className="font-bold text-gray-800">{formatDate(order.createdAt)}</p>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div className="p-8">
          {/* Tabla de productos con diseño mejorado */}
          <div className="mb-8">
            <table className="w-full">
              <thead>
                <tr className="border-b-2 border-blue-200">
                  <th className="text-left py-4 text-sm font-semibold text-blue-700 uppercase tracking-wider">N°</th>
                  <th className="text-left py-4 text-sm font-semibold text-blue-700 uppercase tracking-wider">Descripción del Producto</th>
                  <th className="text-right py-4 text-sm font-semibold text-blue-700 uppercase tracking-wider">Precio</th>
                  <th className="text-right py-4 text-sm font-semibold text-blue-700 uppercase tracking-wider">Cant.</th>
                  <th className="text-right py-4 text-sm font-semibold text-blue-700 uppercase tracking-wider">Total</th>
                </tr>
              </thead>
              <tbody>
                {orderItems && orderItems.length > 0 ? (
                  orderItems.map((item: any, index: number) => (
                    <tr key={index} className="border-b border-gray-100">
                      <td className="py-4 text-gray-600">{index + 1}</td>
                      <td className="py-4">
                        <div>
                          <p className="font-medium text-gray-900">
                            {item.product?.model || 'Producto'} {item.product?.storage}
                          </p>
                          <p className="text-sm text-gray-500">
                            IMEI: {item.product?.imei} • Color: {item.product?.color}
                          </p>
                        </div>
                      </td>
                      <td className="py-4 text-right font-medium">{formatCurrency(item.priceUsd)}</td>
                      <td className="py-4 text-right">{item.quantity}</td>
                      <td className="py-4 text-right font-bold">{formatCurrency(parseFloat(item.priceUsd) * item.quantity)}</td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={5} className="py-8 text-center text-gray-500">
                      No hay productos en este pedido
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          {/* Footer con totales y métodos de pago */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
            {/* Métodos de pago */}
            <div>
              <div className="bg-blue-50 dark:bg-blue-900/20 p-6 rounded-lg">
                <h3 className="font-bold text-blue-600 dark:text-blue-400 mb-4">Gracias por su compra</h3>
                
                <div className="space-y-3">
                  <h4 className="font-semibold text-gray-900 dark:text-white">Información de Pago:</h4>
                  {payments && payments.length > 0 ? (
                    payments.map((payment: any, index: number) => {
                      const isDebtPayment = payment.notes && (payment.notes.includes('Pago parcial de deuda') || payment.notes.includes('Pago final de deuda'));
                      return (
                        <div key={index} className="text-sm border-b border-gray-200 pb-2 mb-2 last:border-b-0 last:pb-0 last:mb-0">
                          <div className="flex items-center justify-between mb-1">
                            <div className="flex items-center gap-2">
                              <p className="font-medium">
                                {formatPaymentMethod(payment.paymentMethod)}: {formatCurrency(payment.amountUsd)}
                              </p>
                              {isDebtPayment && (
                                <span className="text-xs bg-orange-100 text-orange-800 px-2 py-1 rounded">
                                  Pago de Deuda
                                </span>
                              )}
                            </div>
                          </div>
                          <div className="text-xs text-gray-500 mb-1">
                            📅 {formatDate(payment.createdAt)} a las {new Date(payment.createdAt).toLocaleTimeString('es-AR', { hour: '2-digit', minute: '2-digit' })}
                          </div>
                          {payment.exchangeRate && parseFloat(payment.exchangeRate) !== 1 && (
                            <p className="text-gray-500 text-xs">
                              ARS {parseFloat(payment.amount).toLocaleString('es-AR', {minimumFractionDigits: 2})} (TC: {parseFloat(payment.exchangeRate).toLocaleString('es-AR', {maximumFractionDigits: 0})})
                            </p>
                          )}
                          {payment.notes && isDebtPayment && (
                            <div className="text-xs text-gray-400 italic mt-1">
                              {payment.notes}
                            </div>
                          )}
                        </div>
                      );
                    })
                  ) : (
                    <p className="text-gray-500">No hay pagos registrados</p>
                  )}
                </div>
              </div>

              {/* Términos y condiciones */}
              <div className="mt-6">
                <h4 className="font-semibold text-gray-900 dark:text-white mb-2">Términos y Condiciones</h4>
                <p className="text-xs text-gray-500 leading-relaxed">
                  Los productos tienen garantía de 30 días por defectos de fábrica. 
                  No se aceptan devoluciones por daños físicos del usuario.
                  Conserve esta factura como comprobante de compra.
                </p>
              </div>
            </div>

            {/* Totales y Estado de Pago */}
            <div>
              {/* Estado del Pago */}
              <div className={`p-4 rounded-lg border-2 mb-4 ${paymentStatus.bgColor}`}>
                <div className="flex items-center justify-between">
                  <div className="flex items-center">
                    <span className="text-2xl mr-3">{paymentStatus.icon}</span>
                    <div>
                      <h4 className={`font-bold text-lg ${paymentStatus.color}`}>
                        {paymentStatus.label}
                      </h4>
                      <p className="text-sm text-gray-600">
                        Pagado: {formatCurrency(totalPaid)} de {formatCurrency(subtotal)}
                      </p>
                      {remainingDebt <= 0.01 && payments && payments.length > 0 && (
                        <div className="text-xs text-gray-500 mt-1">
                          Finalizado: {formatDate(payments[payments.length - 1].createdAt)} {new Date(payments[payments.length - 1].createdAt).toLocaleTimeString('es-AR', { hour: '2-digit', minute: '2-digit' })}
                        </div>
                      )}
                      {(() => {
                        const debtPayments = payments?.filter((p: any) => p.notes && (p.notes.includes('Pago parcial de deuda') || p.notes.includes('Pago final de deuda'))) || [];
                        if (remainingDebt <= 0.01 && debtPayments.length > 0) {
                          return (
                            <div className="text-xs text-green-600 mt-1">
                              🎉 Deuda cancelada con {debtPayments.length} pago{debtPayments.length > 1 ? 's' : ''} adicional{debtPayments.length > 1 ? 'es' : ''}
                            </div>
                          );
                        }
                        return null;
                      })()}
                    </div>
                  </div>
                  {remainingDebt > 0.01 && (
                    <div className="text-right">
                      <p className="text-sm text-gray-500">Deuda Restante:</p>
                      <p className={`font-bold text-lg ${paymentStatus.color}`}>
                        {formatCurrency(Math.max(0, remainingDebt))}
                      </p>
                    </div>
                  )}
                </div>
              </div>

              <div className="bg-gray-50 dark:bg-gray-700 p-6 rounded-lg">
                <div className="space-y-3">
                  <div className="flex justify-between py-2">
                    <span className="text-gray-600 dark:text-gray-400">Subtotal:</span>
                    <span className="font-medium">{formatCurrency(subtotal)}</span>
                  </div>
                  <div className="flex justify-between py-2">
                    <span className="text-gray-600 dark:text-gray-400">Impuestos:</span>
                    <span className="font-medium">0.00%</span>
                  </div>
                  <div className="border-t-2 border-blue-600 pt-3">
                    <div className="flex justify-between items-center">
                      <span className="text-xl font-bold text-blue-600 dark:text-blue-400">Total:</span>
                      <span className="text-2xl font-bold text-blue-600 dark:text-blue-400">
                        {formatCurrency(subtotal)}
                      </span>
                    </div>
                  </div>
                  <div className="border-t pt-3">
                    <div className="flex justify-between items-center">
                      <span className="text-lg font-semibold text-green-600">Total Pagado:</span>
                      <span className="text-xl font-bold text-green-600">
                        {formatCurrency(totalPaid)}
                      </span>
                    </div>
                  </div>
                  {remainingDebt > 0 && (
                    <div className="border-t pt-3">
                      <div className="flex justify-between items-center">
                        <span className="text-lg font-semibold text-red-600">Saldo Pendiente:</span>
                        <span className="text-xl font-bold text-red-600">
                          {formatCurrency(remainingDebt)}
                        </span>
                      </div>
                    </div>
                  )}
                </div>
              </div>

              {/* Firma autorizada */}
              <div className="mt-6 text-center border-t border-gray-200 pt-4">
                <p className="text-sm text-gray-500 mb-8">Firma Autorizada</p>
                <div className="border-t border-gray-300 mx-8"></div>
              </div>
            </div>
          </div>

          {/* Información de empresa */}
          {companyConfig && (
            <div className="mt-8 pt-6 border-t border-gray-200 text-center">
              <div className="text-sm text-gray-500 space-y-1">
                <p><strong>{companyConfig.companyName}</strong> • CUIT: {companyConfig.cuit}</p>
                <p>{companyConfig.address}</p>
                <p>Email: {companyConfig.email} • Tel: {companyConfig.phone}</p>
                {companyConfig.website && <p>Web: {companyConfig.website}</p>}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}