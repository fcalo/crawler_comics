$('#nuevo').on('show.bs.modal', function (e) {
	var f = new Date();
	$("#start_date").val(f.getDate() + "/" + (f.getMonth() +1) + "/" + f.getFullYear() + " " +f.getHours() + ":" + f.getMinutes()) ;
})

$("#send_contact").click(function(){
	$("#form").submit();
});


/*newsletter */


$('#nuevo_news').on('show.bs.modal', function (e) {
	$("#type_link").val("N");
	$("#affil").hide();
	$(".banner_active").attr("checked", false);
	$(".banner").hide();
})

$("#type_link").click(function(){
	if ($(this).val()=="A")
		$("#affil").fadeIn();
	else
		$("#affil").fadeOut();
});

$(".banner_active").click(function(){
	i = $(this).attr("id").split("_")[1];
	if ($("#banner_" + i + "_active").prop("checked"))
		$(".banner_" + i).fadeIn();
	else
		$(".banner_" + i).fadeOut();
});

$('#date_from').datepicker({
    format: "dd/mm/yyyy",
    language: "es"
});
$('#date_to').datepicker({
    format: "dd/mm/yyyy",
    language: "es"
});


function getTree() {
			
	var tree = []
	var cats = get_categories();
	var last_c = "";
	var nodes = []
	for (category in cats){
		cat = cats[category];
		a = cat.split("/");
		c = a[0];
		sc = a[1]
		
		if (last_c == "")
			last_c = c;
			
			
		if (last_c != c){
			if (last_c=="COMICS")
				tree.push({text:last_c, nodes:nodes});
			else
				tree.push({text:last_c});
				
			var nodes = []
		}
		nodes.push({text:cat});
		
		last_c = c;
	}
	tree.push({text:c});
  
	// Some logic to retrieve, or generate tree structure
	return tree;
}


$('#tree_categories').treeview({
	data: getTree(),
	onNodeSelected: function(event, node) {
		if ($("#categories").val() == "")
			$("#categories").val(node.text) 
		else
			$("#categories").val($("#categories").val() + "," + node.text ) 
	}}
);
