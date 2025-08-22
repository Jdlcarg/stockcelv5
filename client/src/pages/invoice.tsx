import { useState, useEffect } from "react";
import { useLocation, useParams } from "wouter";
import { useAuth } from "@/hooks/use-auth";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import { Printer, Download, ArrowLeft } from "lucide-react";

interface InvoiceParams {
  orderId: string;
}

export default function Invoice() {
  const [, setLocation] = useLocation();
  const { isAuthenticated, user } = useAuth();
  const params = useParams<InvoiceParams>();
  const orderId = params?.orderId;

  useEffect(() => {
    if (!isAuthenticated) {
      setLocation("/");
    }
  }, [isAuthenticated, setLocation]);

  // Obtener datos del pedido
  const { data: order, isLoading: orderLoading } = useQuery({
    queryKey: ['/api/orders', orderId],
    queryFn: async () => {
      const response = await fetch(`/api/orders/${orderId}`);
      if (!response.ok) throw new Error('Failed to fetch order');
      return response.json();
    },
    enabled: !!orderId,
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
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  // Obtener pagos del pedido
  const { data: payments, isLoading: paymentsLoading } = useQuery({
    queryKey: ['/api/payments', orderId],
    queryFn: async () => {
      const response = await fetch(`/api/payments?orderId=${orderId}`);
      if (!response.ok) throw new Error('Failed to fetch payments');
      return response.json();
    },
    enabled: !!orderId,
  });

  // Obtener información de vendedores
  const { data: vendors } = useQuery({
    queryKey: ["/api/users/vendors", user?.clientId],
    queryFn: async () => {
      const response = await fetch(`/api/users/vendors?clientId=${user?.clientId}`);
      return response.json();
    },
    enabled: !!user?.clientId,
  });

  const handlePrint = () => {
    window.print();
  };

  const handleBack = () => {
    setLocation("/orders");
  };

  // Función para obtener nombre del vendedor
  const getVendorName = (vendorId: number) => {
    if (!vendors) return "Cargando...";
    const vendor = vendors.find((v: any) => v.id === vendorId);
    return vendor ? vendor.name : "Usuario desconocido";
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
      year: 'numeric',
      month: 'long',
      day: 'numeric'
    });
  };

  const formatCurrency = (amount: string | number) => {
    return new Intl.NumberFormat('es-AR', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 2
    }).format(typeof amount === 'string' ? parseFloat(amount) : amount);
  };

  const getStatusBadge = (status: string) => {
    const statusConfig = {
      'disponible': { color: 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200', label: 'Disponible' },
      'reservado': { color: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200', label: 'Reservado' },
      'vendido': { color: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200', label: 'Vendido' },
    };
    const config = statusConfig[status as keyof typeof statusConfig] || statusConfig['disponible'];
    return <Badge className={config.color}>{config.label}</Badge>;
  };

  const getPaymentMethodLabel = (method: string) => {
    const methods = {
      'efectivo_ars': 'Efectivo ARS',
      'efectivo_usd': 'Efectivo USD',
      'transferencia_ars': 'Transferencia ARS',
      'transferencia_usdt': 'Transferencia USDT',
      'financiera': 'Financiera'
    };
    return methods[method as keyof typeof methods] || method;
  };

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
      {/* Header para impresión */}
      <div className="print:hidden bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 p-4">
        <div className="max-w-4xl mx-auto flex items-center justify-between">
          <div className="flex items-center space-x-4">
            <Button onClick={handleBack} variant="outline" size="sm">
              <ArrowLeft className="w-4 h-4 mr-2" />
              Volver
            </Button>
            <h1 className="text-xl font-semibold text-gray-900 dark:text-white">
              Factura #{order.orderNumber}
            </h1>
          </div>
          <div className="flex space-x-2">
            <Button onClick={handlePrint} variant="outline" size="sm">
              <Printer className="w-4 h-4 mr-2" />
              Imprimir
            </Button>
          </div>
        </div>
      </div>

      {/* Contenido de la factura */}
      <div className="max-w-4xl mx-auto p-6 print:p-0 print:max-w-none invoice-container">
        <div className="bg-white dark:bg-gray-800 print:bg-white print:shadow-none shadow-lg rounded-lg print:rounded-none overflow-hidden">
          {/* Header de la empresa */}
          <div className="p-8 print:p-6 border-b border-gray-200 dark:border-gray-700 print:border-gray-300">
            <div className="flex items-start justify-between">
              <div className="flex items-start space-x-6">
                {companyConfig?.logoUrl && (
                  <div className="flex-shrink-0">
                    <img 
                      src={companyConfig.logoUrl} 
                      alt="Logo de la empresa" 
                      className="w-20 h-20 object-contain"
                    />
                  </div>
                )}
                <div>
                  <h1 className="text-2xl font-bold text-gray-900 dark:text-white print:text-gray-900">
                    {companyConfig?.companyName || 'Empresa'}
                  </h1>
                  {companyConfig?.cuit && (
                    <p className="text-sm text-gray-600 dark:text-gray-400 print:text-gray-600 mt-1">
                      CUIT: {companyConfig.cuit}
                    </p>
                  )}
                  {companyConfig?.address && (
                    <p className="text-sm text-gray-600 dark:text-gray-400 print:text-gray-600">
                      {companyConfig.address}
                    </p>
                  )}
                  <div className="mt-2 space-y-1">
                    {companyConfig?.phone && (
                      <p className="text-sm text-gray-600 dark:text-gray-400 print:text-gray-600">
                        Tel: {companyConfig.phone}
                      </p>
                    )}
                    {companyConfig?.email && (
                      <p className="text-sm text-gray-600 dark:text-gray-400 print:text-gray-600">
                        Email: {companyConfig.email}
                      </p>
                    )}
                    {companyConfig?.website && (
                      <p className="text-sm text-gray-600 dark:text-gray-400 print:text-gray-600">
                        Web: {companyConfig.website}
                      </p>
                    )}
                  </div>
                </div>
              </div>
              <div className="text-right">
                <h2 className="text-xl font-semibold text-gray-900 dark:text-white print:text-gray-900">
                  FACTURA
                </h2>
                <p className="text-lg font-medium text-blue-600 dark:text-blue-400 print:text-blue-600 mt-1">
                  #{order.orderNumber}
                </p>
                <p className="text-sm text-gray-600 dark:text-gray-400 print:text-gray-600 mt-2">
                  Fecha: {formatDate(order.createdAt)}
                </p>
              </div>
            </div>
          </div>

          {/* Información del cliente */}
          <div className="p-8 print:p-6 border-b border-gray-200 dark:border-gray-700 print:border-gray-300">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div>
                <h3 className="text-lg font-semibold text-gray-900 dark:text-white print:text-gray-900 mb-3">
                  Datos del Cliente
                </h3>
                <div className="space-y-2">
                  <p className="text-gray-900 dark:text-white print:text-gray-900 font-medium">
                    {order.customerName}
                  </p>
                  {order.customerPhone && (
                    <p className="text-sm text-gray-600 dark:text-gray-400 print:text-gray-600">
                      Tel: {order.customerPhone}
                    </p>
                  )}
                  {order.customerEmail && (
                    <p className="text-sm text-gray-600 dark:text-gray-400 print:text-gray-600">
                      Email: {order.customerEmail}
                    </p>
                  )}
                </div>
              </div>
              <div>
                <h3 className="text-lg font-semibold text-gray-900 dark:text-white print:text-gray-900 mb-3">
                  Información del Pedido
                </h3>
                <div className="space-y-2">
                  <div className="flex items-center space-x-2">
                    <span className="text-sm text-gray-600 dark:text-gray-400 print:text-gray-600">Estado:</span>
                    <Badge className="bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200 print:bg-green-100 print:text-green-800">
                      {order.status === 'completado' ? 'Completado' : order.status}
                    </Badge>
                  </div>
                  <div className="flex items-center space-x-2">
                    <span className="text-sm text-gray-600 dark:text-gray-400 print:text-gray-600">Pago:</span>
                    <Badge className="bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200 print:bg-blue-100 print:text-blue-800">
                      {order.paymentStatus === 'pagado' ? 'Pagado' : order.paymentStatus === 'parcial' ? 'Parcial' : 'Pendiente'}
                    </Badge>
                  </div>
                  {order.vendorId && (
                    <div className="flex items-center space-x-2">
                      <span className="text-sm text-gray-600 dark:text-gray-400 print:text-gray-600">Vendedor:</span>
                      <span className="text-sm font-medium text-gray-900 dark:text-white print:text-gray-900">
                        {getVendorName(order.vendorId)}
                      </span>
                    </div>
                  )}
                  {/* Información de entrega */}
                  <div className="flex items-center space-x-2">
                    <span className="text-sm text-gray-600 dark:text-gray-400 print:text-gray-600">Entrega:</span>
                    {order.shippingType === 'oficina' && (
                      <Badge className="bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200 print:bg-blue-100 print:text-blue-800">
                        Retiro en Oficina
                      </Badge>
                    )}
                    {order.shippingType === 'direccion' && (
                      <Badge className="bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200 print:bg-green-100 print:text-green-800">
                        Envío a Domicilio
                      </Badge>
                    )}
                    {!order.shippingType && (
                      <span className="text-sm text-gray-500 dark:text-gray-400 print:text-gray-500 italic">
                        No especificado
                      </span>
                    )}
                  </div>
                  {order.shippingType === 'direccion' && order.shippingAddress && (
                    <div className="mt-2">
                      <span className="text-sm text-gray-600 dark:text-gray-400 print:text-gray-600">Dirección:</span>
                      <p className="text-sm font-medium text-gray-900 dark:text-white print:text-gray-900 mt-1">
                        {order.shippingAddress}
                      </p>
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>

          {/* Tabla de productos */}
          <div className="p-8 print:p-6">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white print:text-gray-900 mb-4">
              Productos
            </h3>
            <div className="w-full overflow-hidden">
              <table className="w-full border-collapse invoice-table">
                <thead>
                  <tr className="border-b-2 border-gray-300 dark:border-gray-600 print:border-gray-400">
                    <th className="text-left py-3 px-2 font-semibold text-gray-900 dark:text-white print:text-gray-900 text-xs col-imei">IMEI</th>
                    <th className="text-left py-3 px-2 font-semibold text-gray-900 dark:text-white print:text-gray-900 text-xs col-model">Modelo</th>
                    <th className="text-left py-3 px-2 font-semibold text-gray-900 dark:text-white print:text-gray-900 text-xs col-storage">Almac.</th>
                    <th className="text-left py-3 px-2 font-semibold text-gray-900 dark:text-white print:text-gray-900 text-xs col-color">Color</th>
                    <th className="text-center py-3 px-2 font-semibold text-gray-900 dark:text-white print:text-gray-900 text-xs col-qty">Cant.</th>
                    <th className="text-right py-3 px-2 font-semibold text-gray-900 dark:text-white print:text-gray-900 text-xs col-price">Precio Unit.</th>
                    <th className="text-right py-3 px-2 font-semibold text-gray-900 dark:text-white print:text-gray-900 text-xs col-total">Total</th>
                  </tr>
                </thead>
                <tbody>
                  {orderItems?.map((item: any, index: number) => (
                    <tr key={index} className="border-b border-gray-200 dark:border-gray-600 print:border-gray-300">
                      <td className="py-2 px-2 text-xs text-gray-900 dark:text-gray-300 print:text-gray-900 col-imei">
                        {item.product?.imei?.slice(-8) || 'N/A'}
                      </td>
                      <td className="py-2 px-2 text-xs text-gray-900 dark:text-gray-300 print:text-gray-900 col-model">
                        {item.product?.model || 'N/A'}
                      </td>
                      <td className="py-2 px-2 text-xs text-gray-900 dark:text-gray-300 print:text-gray-900 col-storage">
                        {item.product?.storage || 'N/A'}
                      </td>
                      <td className="py-2 px-2 text-xs text-gray-900 dark:text-gray-300 print:text-gray-900 col-color">
                        {item.product?.color || 'N/A'}
                      </td>
                      <td className="py-2 px-2 text-xs text-center text-gray-900 dark:text-gray-300 print:text-gray-900 col-qty">
                        {item.quantity}
                      </td>
                      <td className="py-2 px-2 text-xs text-right text-gray-900 dark:text-gray-300 print:text-gray-900 col-price">
                        {formatCurrency(item.priceUsd)}
                      </td>
                      <td className="py-2 px-2 text-xs text-right font-semibold text-gray-900 dark:text-white print:text-gray-900 col-total">
                        {formatCurrency(parseFloat(item.priceUsd) * item.quantity)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Total */}
            <div className="mt-6 border-t-2 border-gray-300 dark:border-gray-600 print:border-gray-400 pt-4">
              <div className="flex justify-end">
                <div className="bg-blue-50 dark:bg-blue-900 print:bg-blue-50 rounded-lg p-4 min-w-64">
                  <div className="space-y-2">
                    <div className="flex justify-between items-center">
                      <span className="text-sm text-gray-600 dark:text-gray-400 print:text-gray-600">Subtotal:</span>
                      <span className="text-sm font-medium text-gray-900 dark:text-white print:text-gray-900">
                        {formatCurrency(order.totalUsd)}
                      </span>
                    </div>
                    <div className="border-t border-blue-200 dark:border-blue-700 print:border-blue-200 pt-2">
                      <div className="flex justify-between items-center">
                        <span className="text-lg font-bold text-blue-900 dark:text-blue-100 print:text-blue-900">
                          TOTAL USD:
                        </span>
                        <span className="text-xl font-bold text-blue-900 dark:text-blue-100 print:text-blue-900">
                          {formatCurrency(order.totalUsd)}
                        </span>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>

            {/* Detalle de Pagos */}
            {payments && payments.length > 0 && (
              <div className="mt-6 border-t border-gray-200 dark:border-gray-700 print:border-gray-300 pt-4">
                <h4 className="text-md font-semibold text-gray-900 dark:text-white print:text-gray-900 mb-4">
                  Detalle de Pagos
                </h4>
                <div className="bg-gray-50 dark:bg-gray-800 print:bg-gray-50 rounded-lg p-4 space-y-3">
                  {payments.map((payment: any, index: number) => {
                    const exchangeRate = parseFloat(payment.exchangeRate) || 1;
                    const localAmount = parseFloat(payment.amount) || 0;
                    const usdAmount = parseFloat(payment.amountUsd) || 0;
                    const isARS = payment.paymentMethod.includes('_ars');
                    const isDebtPayment = payment.notes && payment.notes.includes('Pago parcial de deuda') || payment.notes && payment.notes.includes('Pago final de deuda');
                    
                    return (
                      <div key={index} className="border-b border-gray-200 dark:border-gray-600 print:border-gray-300 pb-3 last:border-b-0 last:pb-0 mb-3 last:mb-0">
                        <div className="flex justify-between items-start">
                          <div className="flex-1 space-y-1">
                            <div className="flex items-center justify-between">
                              <div className="space-y-1">
                                <div className="flex items-center gap-2">
                                  <span className="text-sm font-medium text-gray-900 dark:text-white print:text-gray-900">
                                    {getPaymentMethodLabel(payment.paymentMethod)}
                                  </span>
                                  {isDebtPayment && (
                                    <span className="text-xs bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200 print:bg-orange-100 print:text-orange-800 px-2 py-1 rounded">
                                      Pago de Deuda
                                    </span>
                                  )}
                                </div>
                                <div className="text-xs text-gray-600 dark:text-gray-400 print:text-gray-600">
                                  📅 {formatDate(payment.createdAt)} a las {new Date(payment.createdAt).toLocaleTimeString('es-AR', { hour: '2-digit', minute: '2-digit' })}
                                </div>
                                {payment.notes && isDebtPayment && (
                                  <div className="text-xs text-gray-500 dark:text-gray-400 print:text-gray-500 italic">
                                    {payment.notes}
                                  </div>
                                )}
                              </div>
                              <div className="text-right">
                                {isARS ? (
                                  <div className="space-y-1">
                                    <div className="text-sm text-gray-900 dark:text-white print:text-gray-900">
                                      ARS ${localAmount.toLocaleString('es-AR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}
                                    </div>
                                    <div className="text-xs text-gray-600 dark:text-gray-400 print:text-gray-600">
                                      Equivale: {formatCurrency(usdAmount)} (TC: {exchangeRate.toLocaleString()})
                                    </div>
                                  </div>
                                ) : (
                                  <div className="text-sm font-medium text-gray-900 dark:text-white print:text-gray-900">
                                    {formatCurrency(usdAmount)}
                                  </div>
                                )}
                              </div>
                            </div>
                          </div>
                        </div>
                      </div>
                    );
                  })}
                  
                  {/* Total pagado y estado */}
                  <div className="border-t-2 border-gray-300 dark:border-gray-600 print:border-gray-400 pt-3 mt-4">
                    <div className="space-y-2">
                      <div className="flex justify-between items-center">
                        <span className="text-sm font-semibold text-gray-900 dark:text-white print:text-gray-900">
                          Total Pagado:
                        </span>
                        <span className="text-sm font-bold text-gray-900 dark:text-white print:text-gray-900">
                          {formatCurrency(payments.reduce((sum: number, p: any) => sum + parseFloat(p.amountUsd || '0'), 0))}
                        </span>
                      </div>
                      
                      {/* Estado de pago con fecha de finalización */}
                      {(() => {
                        const totalPaid = payments.reduce((sum: number, p: any) => sum + parseFloat(p.amountUsd || '0'), 0);
                        const totalOrder = parseFloat(order.totalUsd);
                        const remainingDebt = totalOrder - totalPaid;
                        const debtPayments = payments.filter((p: any) => p.notes && (p.notes.includes('Pago parcial de deuda') || p.notes.includes('Pago final de deuda')));
                        const lastPayment = payments.length > 0 ? payments[payments.length - 1] : null;
                        
                        if (remainingDebt <= 0.01) {
                          return (
                            <div className="bg-green-50 dark:bg-green-900/20 print:bg-green-50 border border-green-200 dark:border-green-800 print:border-green-200 rounded p-3">
                              <div className="flex items-center justify-between">
                                <div className="flex items-center gap-2">
                                  <span className="text-green-600 dark:text-green-400 print:text-green-600">✅</span>
                                  <span className="text-sm font-semibold text-green-800 dark:text-green-200 print:text-green-800">
                                    PAGADO COMPLETO
                                  </span>
                                </div>
                                {lastPayment && (
                                  <div className="text-xs text-green-600 dark:text-green-400 print:text-green-600">
                                    Finalizado: {formatDate(lastPayment.createdAt)} {new Date(lastPayment.createdAt).toLocaleTimeString('es-AR', { hour: '2-digit', minute: '2-digit' })}
                                  </div>
                                )}
                              </div>
                              {debtPayments.length > 0 && (
                                <div className="text-xs text-green-600 dark:text-green-400 print:text-green-600 mt-1">
                                  🎉 Deuda cancelada con {debtPayments.length} pago{debtPayments.length > 1 ? 's' : ''} adicional{debtPayments.length > 1 ? 'es' : ''}
                                </div>
                              )}
                            </div>
                          );
                        } else if (totalPaid > 0) {
                          return (
                            <div className="bg-yellow-50 dark:bg-yellow-900/20 print:bg-yellow-50 border border-yellow-200 dark:border-yellow-800 print:border-yellow-200 rounded p-3">
                              <div className="flex items-center justify-between">
                                <div className="flex items-center gap-2">
                                  <span className="text-yellow-600 dark:text-yellow-400 print:text-yellow-600">⚠️</span>
                                  <span className="text-sm font-semibold text-yellow-800 dark:text-yellow-200 print:text-yellow-800">
                                    PAGO PARCIAL
                                  </span>
                                </div>
                                <div className="text-xs text-yellow-600 dark:text-yellow-400 print:text-yellow-600">
                                  Saldo pendiente: {formatCurrency(remainingDebt)}
                                </div>
                              </div>
                            </div>
                          );
                        } else {
                          return (
                            <div className="bg-red-50 dark:bg-red-900/20 print:bg-red-50 border border-red-200 dark:border-red-800 print:border-red-200 rounded p-3">
                              <div className="flex items-center gap-2">
                                <span className="text-red-600 dark:text-red-400 print:text-red-600">❌</span>
                                <span className="text-sm font-semibold text-red-800 dark:text-red-200 print:text-red-800">
                                  NO PAGADO
                                </span>
                              </div>
                            </div>
                          );
                        }
                      })()}
                    </div>
                  </div>
                </div>
              </div>
            )}

            {/* Observaciones */}
            {order.observations && (
              <div className="mt-6 border-t border-gray-200 dark:border-gray-700 print:border-gray-300 pt-4">
                <h4 className="text-md font-semibold text-gray-900 dark:text-white print:text-gray-900 mb-2">
                  Observaciones
                </h4>
                <p className="text-sm text-gray-600 dark:text-gray-400 print:text-gray-600">
                  {order.observations}
                </p>
              </div>
            )}

            {/* Footer */}
            <div className="mt-8 pt-4 border-t border-gray-200 dark:border-gray-700 print:border-gray-300 text-center">
              <p className="text-xs text-gray-500 dark:text-gray-400 print:text-gray-500">
                Gracias por su compra. Esta es una factura generada electrónicamente.
              </p>
              {companyConfig?.taxCondition && (
                <p className="text-xs text-gray-500 dark:text-gray-400 print:text-gray-500 mt-1">
                  Condición fiscal: {companyConfig.taxCondition}
                </p>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}