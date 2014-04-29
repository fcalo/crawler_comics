$('#nuevo').on('show.bs.modal', function (e) {
	var f = new Date();
	$("#start_date").val(f.getDate() + "/" + (f.getMonth() +1) + "/" + f.getFullYear() + " " +f.getHours() + ":" + f.getMinutes()) ;
})

$("#send_contact").click(function(){
	$("#form").submit();
});
